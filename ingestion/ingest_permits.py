"""
ingest_permits.py
-----------------
Ingests Volusia County AMANDA open permit records from the ArcGIS
MapServer into raw.raw_permits in PostgreSQL.

Columns retained (7 of 15)
---------------------------
FOLDERRSN       -- unique permit identifier — upsert key
FOLDERTYPE      -- permit type code (RES, ELEC, PLMB, etc.)
FOLDERNAME      -- permit number as displayed
STATUSDESC      -- current status (Issued, Plan Review, etc.)
INDATE          -- application submitted date — processing speed metric
ALTKEY          -- parcel key — joins to raw_cama_parcel.parid
COUNCILDISTRICT -- council district — geographic grouping

Dropped columns
---------------
FOLDERGROUP, FOLDERDESCRIPTION, STATUSCODE, PID, SUBNUM,
REFERENCEFILE, FOLDERLINK, folderdesc, SYMBOL, objectid
These add no analytical value for permit velocity or
neighborhood analysis.

Load strategy
-------------
UPSERT on FOLDERRSN — permits are live records that change status.
Re-running the script updates changed permits rather than duplicating.
First run creates the table and unique constraint. Subsequent runs
upsert against it.

Geographic limitation
---------------------
Unincorporated Volusia County only. City of Daytona Beach permits
are issued through a separate system and are NOT included.

Run modes
---------
Full load:  python ingest_permits.py
Dry run:    python ingest_permits.py --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from arcgis_client import ArcGISClient
from config import Endpoints, Pipeline, settings

logger = logging.getLogger("volusia.ingestion")

# ---------------------------------------------------------------------------
# Columns to keep from the raw API response
# ---------------------------------------------------------------------------
KEEP_COLUMNS = [
    "FOLDERRSN",
    "FOLDERTYPE",
    "FOLDERNAME",
    "STATUSDESC",
    "INDATE",
    "ALTKEY",
    "COUNCILDISTRICT",
]

UPSERT_KEY = "folderrsn"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_raw_schema(engine) -> None:
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.schema}"))
        conn.commit()
    logger.info("Schema confirmed: %s", settings.schema)


def get_row_count(engine, table: str, schema: str) -> int:
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
            return result.scalar()
    except Exception:  # noqa: BLE001
        return 0


def ensure_upsert_constraint(engine, schema: str, table: str) -> None:
    """Add unique constraint on folderrsn if it does not already exist."""
    constraint_name = f"uq_{table}_folderrsn"
    sql = text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{constraint_name}'
            ) THEN
                ALTER TABLE {schema}.{table}
                ADD CONSTRAINT {constraint_name} UNIQUE (folderrsn);
            END IF;
        END $$;
    """)
    with engine.connect() as conn:
        conn.execute(sql)
        conn.commit()
    logger.info("Upsert constraint confirmed on %s.%s", schema, table)


def upsert_permits(df: pd.DataFrame, engine, schema: str, table: str) -> None:
    """
    Insert permit records, updating existing rows on FOLDERRSN conflict.

    Uses a staging temp table to avoid row-level locking on large updates.
    """
    temp_table = f"{table}_staging_temp"
    non_key_cols = [c for c in df.columns if c != UPSERT_KEY]
    update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in non_key_cols)

    with engine.connect() as conn:
        df.to_sql(
            name=temp_table,
            con=conn,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=Pipeline.DB_CHUNKSIZE,
            method="multi",
        )
        conn.execute(text(f"""
            INSERT INTO {schema}.{table}
            SELECT * FROM {schema}.{temp_table}
            ON CONFLICT ({UPSERT_KEY})
            DO UPDATE SET {update_set}
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
        conn.commit()


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retain only the columns defined in KEEP_COLUMNS.
    Warns if any expected column is missing from the API response.
    """
    available = [c for c in KEEP_COLUMNS if c in df.columns]
    missing   = [c for c in KEEP_COLUMNS if c not in df.columns]
    if missing:
        logger.warning("Expected columns not in API response: %s", missing)
    return df[available]


def add_ingestion_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"]      = "arcgis_amanda_permits"
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("Permits ingestion started")
    logger.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("Target: %s.%s", settings.schema, Pipeline.RAW_TABLE_PERMITS)
    logger.info("Note: Unincorporated Volusia County only")
    logger.info("=" * 60)

    client = ArcGISClient(
        request_delay=Pipeline.ARCGIS_REQUEST_DELAY,
        timeout=Pipeline.ARCGIS_TIMEOUT,
        max_retries=Pipeline.ARCGIS_MAX_RETRIES,
    )

    # Step 1: Pre-flight count
    logger.info("Step 1/5 — Pre-flight record count")
    expected_count = client.fetch_record_count(Endpoints.PERMITS)
    logger.info(
        "API reports %s records available",
        f"{expected_count:,}" if expected_count >= 0 else "unknown",
    )
    if expected_count == 0:
        logger.error("API returned 0 records. Aborting.")
        sys.exit(1)

    # Step 2: Fetch
    logger.info("Step 2/5 — Fetching permit records")
    df = client.fetch_all(endpoint=Endpoints.PERMITS, label="permits")
    if df.empty:
        logger.error("Fetch returned empty DataFrame. Aborting.")
        sys.exit(1)
    logger.info("Fetched %s rows x %s columns", f"{len(df):,}", len(df.columns))

    # Step 3: Select columns, filter, prepare
    logger.info("Step 3/5 — Selecting columns and preparing for load")
    df = select_columns(df)
    df = normalize_column_names(df)
    df = add_ingestion_metadata(df)

    if "statusdesc" in df.columns:
        logger.info("Status breakdown:\n%s", df["statusdesc"].value_counts().to_string())

    logger.info("Data prepared — %s rows x %s cols", f"{len(df):,}", len(df.columns))

    if dry_run:
        logger.info("DRY RUN — skipping database write.")
        logger.info("\n%s", df.head(3).to_string())
        logger.info("Dry run complete.")
        return

    # Step 4: Load
    logger.info("Step 4/5 — Connecting to PostgreSQL")
    engine = create_engine(settings.sqlalchemy_url)
    ensure_raw_schema(engine)

    rows_before = get_row_count(engine, Pipeline.RAW_TABLE_PERMITS, settings.schema)
    logger.info("Rows before load: %s", f"{rows_before:,}")

    logger.info("Step 5/5 — Writing to %s.%s", settings.schema, Pipeline.RAW_TABLE_PERMITS)

    if rows_before == 0:
        df.to_sql(
            name=Pipeline.RAW_TABLE_PERMITS,
            con=engine,
            schema=settings.schema,
            if_exists=Pipeline.DB_IF_EXISTS_APPEND,
            index=False,
            chunksize=Pipeline.DB_CHUNKSIZE,
            method="multi",
        )
        ensure_upsert_constraint(engine, settings.schema, Pipeline.RAW_TABLE_PERMITS)
    else:
        ensure_upsert_constraint(engine, settings.schema, Pipeline.RAW_TABLE_PERMITS)
        upsert_permits(df, engine, settings.schema, Pipeline.RAW_TABLE_PERMITS)

    # Step 5: Verify
    rows_after   = get_row_count(engine, Pipeline.RAW_TABLE_PERMITS, settings.schema)
    rows_written = rows_after - rows_before

    logger.info("=" * 60)
    logger.info("Permits ingestion complete")
    logger.info("  Rows fetched  : %s", f"{len(df):,}")
    logger.info("  Net new rows  : %s", f"{rows_written:,}")
    logger.info("  Total in table: %s", f"{rows_after:,}")
    logger.info("  Verification  : %s", "OK" if rows_after >= len(df) * 0.95 else "WARNING — check load")
    logger.info("=" * 60)

    engine.dispose()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Volusia County AMANDA permit records into PostgreSQL."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Fetch and validate without writing to the database.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run)
