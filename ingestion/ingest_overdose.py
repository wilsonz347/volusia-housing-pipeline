"""
ingest_overdose.py
------------------
Ingests Volusia County overdose analysis data from the ArcGIS
MapServer into raw.raw_overdose_analysis in PostgreSQL.

Source
------
Volusia County OverdoseAnalysis MapServer — Layer 10
URL: maps5.vcgov.org/arcgis/rest/services/OverdoseAnalysis/MapServer/10

What this data contains
-----------------------
ZIP-level overdose counts for 2021-2024, split by:
- Total overdoses (all substances)
- Opioid-specific overdoses

Why this complements existing data
-----------------------------------
CAMA tables identify investor ownership concentration by ZIP.
Census ACS identifies income and rent burden by ZIP.
This layer adds a public health outcome at the same geographic unit.

The analytical question this enables:
"Do ZIP codes with higher investor ownership concentration also
show higher overdose rates — and has that relationship changed
as investor activity accelerated post-2020?"

That cross-dataset finding — property data + health outcome —
is the Phase 5 analytical centerpiece of this project.

Columns retained (10 of 13)
----------------------------
ZIP_CODE    -- join key to all other ZIP-level data
PO_NAME     -- ZIP name (e.g. "DAYTONA BEACH")
Total2021-2024  -- total overdoses per year
Opiod2021-2024  -- opioid overdoses per year

Dropped: OBJECTID, Shape.STArea(), Shape.STLength()

Load strategy
-------------
REPLACE — small static dataset (~30 rows). Full reload each run.

Run modes
---------
Full load:  python ingest_overdose.py
Dry run:    python ingest_overdose.py --dry-run
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
# Columns to keep
# ---------------------------------------------------------------------------
KEEP_COLUMNS = [
    "ZIP_CODE",
    "PO_NAME",
    "Total2021", "Total2022", "Total2023", "Total2024",
    "Opiod2021", "Opiod2022", "Opiod2023", "Opiod2024",
]


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in KEEP_COLUMNS if c in df.columns]
    missing   = [c for c in KEEP_COLUMNS if c not in df.columns]
    if missing:
        logger.warning("Expected columns not found: %s", missing)
    return df[available]


def add_ingestion_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"]      = "vcgov_overdose_analysis"
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("Overdose analysis ingestion started")
    logger.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("Target: %s.%s", settings.schema, Pipeline.RAW_TABLE_OVERDOSE)
    logger.info("=" * 60)

    client = ArcGISClient(
        request_delay=Pipeline.ARCGIS_REQUEST_DELAY,
        timeout=Pipeline.ARCGIS_TIMEOUT,
        max_retries=Pipeline.ARCGIS_MAX_RETRIES,
    )

    # Step 1: Pre-flight count
    logger.info("Step 1/4 — Pre-flight record count")
    expected_count = client.fetch_record_count(Endpoints.OVERDOSE_ANALYSIS)
    logger.info("API reports %s records available", expected_count)

    if expected_count == 0:
        logger.error("API returned 0 records. Aborting.")
        sys.exit(1)

    # Step 2: Fetch
    logger.info("Step 2/4 — Fetching overdose data")
    df = client.fetch_all(
        endpoint=Endpoints.OVERDOSE_ANALYSIS,
        label="overdose_analysis",
    )

    if df.empty:
        logger.error("Fetch returned empty DataFrame. Aborting.")
        sys.exit(1)

    logger.info("Fetched %s rows x %s columns", len(df), len(df.columns))

    # Step 3: Select columns and prepare
    logger.info("Step 3/4 — Selecting columns and preparing for load")
    df = select_columns(df)
    df = normalize_column_names(df)
    df = add_ingestion_metadata(df)

    # Log the full dataset — it's small enough to print entirely
    logger.info("Full dataset:\n%s", df.to_string())

    if dry_run:
        logger.info("DRY RUN — skipping database write.")
        logger.info("Dry run complete.")
        return

    # Step 4: Load
    logger.info("Step 4/4 — Loading into PostgreSQL")
    engine = create_engine(settings.sqlalchemy_url)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.schema}"))
        conn.commit()

    df.to_sql(
        name=Pipeline.RAW_TABLE_OVERDOSE,
        con=engine,
        schema=settings.schema,
        if_exists=Pipeline.DB_IF_EXISTS_REPLACE,
        index=False,
        chunksize=Pipeline.DB_CHUNKSIZE,
        method="multi",
    )

    with engine.connect() as conn:
        rows_loaded = conn.execute(
            text(f"SELECT COUNT(*) FROM {settings.schema}.{Pipeline.RAW_TABLE_OVERDOSE}")
        ).scalar()

    logger.info("=" * 60)
    logger.info("Overdose ingestion complete")
    logger.info("  Rows loaded : %s", rows_loaded)
    logger.info("  Table       : %s.%s", settings.schema, Pipeline.RAW_TABLE_OVERDOSE)
    logger.info("=" * 60)

    engine.dispose()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Volusia County overdose analysis data into PostgreSQL."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Fetch without writing to the database.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run)
