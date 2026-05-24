"""
ingest_parcels.py
-----------------
Ingests Volusia County parcel records from the ArcGIS FeatureServer
into the raw_parcel_ownership table in PostgreSQL.

What this script does
---------------------
1. Connects to the Volusia County ArcGIS FeatureServer (parcels layer).
2. Fetches all ~300,000 parcel records via paginated requests.
3. Appends a metadata column (ingested_at) so every row is traceable
   to the run that produced it.
4. Loads the records into the raw_parcel_ownership table in PostgreSQL.
5. Logs a summary so the GitHub Actions run log is readable.

Run modes
---------
Full load (default — used for the initial historical load):
    python ingest_parcels.py

Dry run (fetches data but does not write to the database):
    python ingest_parcels.py --dry-run

The table write strategy is always APPEND. The raw layer is
append-only by design — we never overwrite historical ingestion
runs. Each run adds a new snapshot identified by ingested_at.
dbt downstream handles deduplication by selecting the latest
ingested_at per parcel.

Architecture note
-----------------
This script is intentionally thin. All business logic — filtering,
classification, joins — belongs in dbt models. This script's only
job is to move bytes from the API to the database reliably.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from arcgis_client import ArcGISClient
from config import Endpoints, Pipeline, settings

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("volusia.ingestion")


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------

# These are the parcel fields we confirm exist after fetching.
# If any are missing, we log a warning — the field may have been renamed
# in a county system update. We never drop columns silently.
#
# Source: Volusia County Property Appraiser / Florida DOR NAL specification.
# Full field documentation: phase1_data_understanding.docx, Section 2.
EXPECTED_FIELDS = [
    # Identification
    "PARID",        # parcel ID — join key across datasets
    "ALTKEY",       # alternate key
    "PID",          # parcel ID component
    # Classification
    "PC",           # property class code (replaces DOR_UC)
    "PC_DESC",      # property class description
    "LANDCLASS",    # land classification
    # Valuation
    "TOTJUST",      # total just value (replaces JV)
    "LANDJUST",     # land just value
    "IMPRJUST",     # improvement just value
    "STXBL",        # school taxable value
    "NSTXBL",       # non-school taxable value
    # Physical
    "BLDGCOUNT",    # building count
    "LIVUNIT",      # living units (replaces NO_RES_UNTS)
    "RES_BEDROOM",
    "RES_BATHROOM",
    "RES_TOTAL_SFLA",
    "CALCACRES",
    # Ownership
    "OWNER1",       # primary owner name (replaces OWN_NAME)
    "OWNER2",       # secondary owner name
    "MAILADDR1",    # mailing address line 1 (replaces OWN_ADDR1)
    "MAILADDR2",
    "MAILADDR3",
    "MAILCITY",     # mailing city (replaces OWN_CITY)
    "MAILSTATE",    # mailing state (replaces OWN_STATE_DOM)
    "MAILZIP",      # mailing ZIP
    "MAILCOUNTRY",  # foreign country flag
    # Location
    "ADDRFULL",     # full physical address (replaces PHY_ADDR1)
    "CITYNAME",     # physical city
    "ZIP1",         # physical ZIP (replaces PHY_ZIPCD)
    # Sale
    "LASTSALEDT",   # last sale date (replaces SALE_PRC1 date)
    "LASTSALEPRICE",# last sale price (replaces SALE_PRC1)
    # Homestead
    "HXFLAG",       # homestead exemption flag (replaces EXMPT_01)
    # Geography
    "NBHD",         # neighborhood code
    "NBHD_DESC",    # neighborhood description
    "TAXDIST",      # tax district
]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_raw_schema(engine) -> None:
    """
    Create the raw schema if it does not already exist.

    Supabase provisions a 'public' schema by default. We write all
    ingestion output to 'raw' to keep it cleanly separated from
    dbt-managed schemas (staging, intermediate, marts).

    Parameters
    ----------
    engine : sqlalchemy.Engine
        Active SQLAlchemy engine connected to PostgreSQL.
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.schema}"))
        conn.commit()
    logger.info("Schema confirmed: %s", settings.schema)


def get_row_count(engine, table: str, schema: str) -> int:
    """
    Return the current row count of a table. Used for pre/post load
    comparison to confirm rows were written as expected.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        Active SQLAlchemy engine.
    table : str
        Table name.
    schema : str
        Schema name.

    Returns
    -------
    int
        Row count, or 0 if the table does not exist.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{table}")
            )
            return result.scalar()
    except Exception:  # noqa: BLE001
        return 0


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def validate_fields(df: pd.DataFrame) -> None:
    """
    Warn if any expected fields are absent from the fetched DataFrame.

    Missing fields indicate the county may have renamed or removed
    a column in a FeatureServer update. This does not halt ingestion —
    we load what we have and document the gap — but it should prompt
    a review of the dbt staging model.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame returned by ArcGISClient.fetch_all().
    """
    actual_columns = set(df.columns.str.upper())
    expected_columns = set(EXPECTED_FIELDS)
    missing = expected_columns - actual_columns

    if missing:
        logger.warning(
            "Expected fields not found in API response: %s. "
            "Review the dbt staging model for stg_parcels.",
            sorted(missing),
        )
    else:
        logger.info("Field validation passed — all expected fields present.")


def add_ingestion_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Append pipeline metadata columns to the DataFrame before loading.

    These columns are not part of the source data — they are added
    by the pipeline so every row is traceable to the run that produced it.

    Columns added
    -------------
    ingested_at : datetime (UTC)
        Timestamp of this ingestion run. Used by dbt to identify the
        latest snapshot when deduplicating across multiple runs.
    source : str
        Identifies the data source. Used for lineage in the raw schema.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from the API.

    Returns
    -------
    pd.DataFrame
        DataFrame with metadata columns appended.
    """
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"] = "arcgis_parcels"
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase all column names for PostgreSQL compatibility.

    PostgreSQL folds unquoted identifiers to lowercase. ArcGIS returns
    field names in UPPERCASE. Normalizing here means dbt models can
    reference columns without quoting (SELECT parcel_id vs "PARCEL_ID").

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with original ArcGIS column names.

    Returns
    -------
    pd.DataFrame
        DataFrame with lowercased column names.
    """
    df.columns = df.columns.str.lower()
    return df


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    """
    Execute the full parcel ingestion pipeline.

    Parameters
    ----------
    dry_run : bool
        If True, fetches data and validates it but does not write
        to the database. Useful for testing API connectivity and
        field validation without side effects. Default False.
    """
    logger.info("=" * 60)
    logger.info("Parcel ingestion started")
    logger.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("Target: %s.%s", settings.schema, Pipeline.RAW_TABLE_PARCELS)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Pre-flight record count
    # Logs how many records the API reports before we start fetching.
    # If this number looks wrong (e.g. suddenly drops from 300k to 50k),
    # abort before writing anything.
    # ------------------------------------------------------------------
    client = ArcGISClient(
        request_delay=Pipeline.ARCGIS_REQUEST_DELAY,
        timeout=Pipeline.ARCGIS_TIMEOUT,
        max_retries=Pipeline.ARCGIS_MAX_RETRIES,
    )

    logger.info("Step 1/5 — Pre-flight record count")
    expected_count = client.fetch_record_count(Endpoints.PARCEL_OWNERSHIP)
    logger.info(
        "API reports %s records available",
        f"{expected_count:,}" if expected_count >= 0 else "unknown",
    )

    if expected_count == 0:
        logger.error(
            "API returned 0 records. Aborting — something is wrong with the endpoint."
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 2: Fetch all records
    # ------------------------------------------------------------------
    logger.info("Step 2/5 — Fetching all parcel records (this takes ~5 minutes)")
    df = client.fetch_all(
        endpoint=Endpoints.PARCEL_OWNERSHIP,
        label="parcel_ownership",
    )

    if df.empty:
        logger.error("Fetch returned an empty DataFrame. Aborting.")
        sys.exit(1)

    logger.info("Fetched %s rows x %s columns", f"{len(df):,}", len(df.columns))

    # ------------------------------------------------------------------
    # Step 3: Validate and prepare
    # ------------------------------------------------------------------
    logger.info("Step 3/5 — Validating fields and preparing for load")
    validate_fields(df)
    df = normalize_column_names(df)
    df = add_ingestion_metadata(df)
    logger.info("Data prepared — %s rows ready to load", f"{len(df):,}")

    if dry_run:
        logger.info("DRY RUN — skipping database write.")
        logger.info("Sample (first 3 rows):")
        logger.info("\n%s", df.head(3).to_string())
        logger.info("Dry run complete.")
        return

    # ------------------------------------------------------------------
    # Step 4: Load to PostgreSQL
    # ------------------------------------------------------------------
    logger.info("Step 4/5 — Connecting to PostgreSQL")
    engine = create_engine(settings.sqlalchemy_url)
    ensure_raw_schema(engine)

    rows_before = get_row_count(engine, Pipeline.RAW_TABLE_PARCELS, settings.schema)
    logger.info(
        "Rows in %s.%s before load: %s",
        settings.schema,
        Pipeline.RAW_TABLE_PARCELS,
        f"{rows_before:,}",
    )

    logger.info(
        "Step 5/5 — Writing %s rows to %s.%s (chunksize=%s)",
        f"{len(df):,}",
        settings.schema,
        Pipeline.RAW_TABLE_PARCELS,
        Pipeline.DB_CHUNKSIZE,
    )

    df.to_sql(
        name=Pipeline.RAW_TABLE_PARCELS,
        con=engine,
        schema=settings.schema,
        if_exists=Pipeline.DB_IF_EXISTS_APPEND, # type: ignore
        index=False,
        chunksize=Pipeline.DB_CHUNKSIZE,
        method="multi",       # Batch INSERT — much faster than row-by-row.
    )

    # ------------------------------------------------------------------
    # Step 5: Post-load verification
    # ------------------------------------------------------------------
    rows_after = get_row_count(engine, Pipeline.RAW_TABLE_PARCELS, settings.schema)
    rows_written = rows_after - rows_before

    logger.info("=" * 60)
    logger.info("Parcel ingestion complete")
    logger.info("  Rows fetched from API : %s", f"{len(df):,}")
    logger.info("  Rows written to DB    : %s", f"{rows_written:,}")
    logger.info("  Total rows in table   : %s", f"{rows_after:,}")

    # Warn if there is a meaningful discrepancy between fetched and written.
    # Small differences (< 1%) can occur due to duplicate objectids.
    # Large differences indicate a load failure worth investigating.
    if rows_written < len(df) * 0.99:
        logger.warning(
            "Rows written (%s) is more than 1%% less than rows fetched (%s). "
            "Investigate for partial load failure.",
            f"{rows_written:,}",
            f"{len(df):,}",
        )
    else:
        logger.info("  Load verification     : OK")

    logger.info("=" * 60)
    engine.dispose()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Volusia County parcel records into PostgreSQL."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Fetch and validate data without writing to the database.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run)
