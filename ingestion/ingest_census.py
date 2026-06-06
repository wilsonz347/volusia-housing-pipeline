"""
ingest_census.py
----------------
Fetches U.S. Census Bureau ACS 5-Year Estimates for Florida ZIP Code
Tabulation Areas (ZCTAs) and loads them into raw.raw_census_acs in PostgreSQL.

Why this data — given what we already have
------------------------------------------
CAMA tables provide property values, ownership, and exemptions.
Permits provides construction activity.
Neither source contains income, rent, or tenure context.

Census ACS fills three analytical gaps:

1. Affordability ratio — median home value (Census) vs APRTOT (CAMA)
   at the ZIP level. If Census reports $180k median but CAMA shows
   investor purchases averaging $280k, the gap quantifies displacement risk.

2. Rental burden — median gross rent vs median household income.
   High rent-to-income ratios in the same ZIPs showing high investor
   concentration is the cross-dataset finding this project builds toward.

3. Tenure cross-check — Census owner/renter split vs our HX_FLAG
   homestead count from CAMA. Discrepancies are analytically interesting
   and worth documenting in dbt.

Variables fetched
-----------------
B19013_001E  Median household income
B01003_001E  Total population
B25064_001E  Median gross rent
B25077_001E  Median home value (owner-occupied)
B25003_002E  Owner-occupied housing units
B25003_003E  Renter-occupied housing units
B25002_002E  Occupied housing units
B25002_003E  Vacant housing units

Geographic scope
----------------
All Florida ZCTAs fetched, then filtered to Volusia County ZIPs.
The Census API does not support state-level filtering for ZCTAs —
national fetch is required, then we filter in Python.

Volusia County ZIPs covered:
32114, 32117, 32118, 32119, 32124, 32125, 32127, 32128  (Daytona Beach)
32130, 32132, 32141, 32168, 32169, 32174, 32176, 32180  (surrounding areas)
32190, 32198, 32713, 32720, 32721, 32722, 32723, 32724
32725, 32726, 32728, 32732, 32738, 32744, 32759, 32763
32764, 32168, 32771

Load strategy
-------------
TRUNCATE + INSERT.

ACS 5-year estimates update annually (December release).
The source is a full snapshot, so each run truncates the
existing table and reloads all rows. The table itself is
preserved to avoid breaking downstream views.

Run modes
---------
Full load:  python ingest_census.py
Dry run:    python ingest_census.py --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine, text, inspect

from config import CENSUS_API_KEY, Endpoints, Pipeline, settings

logger = logging.getLogger("volusia.ingestion")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ACS variables to fetch and their human-readable names.
# Keys are Census variable codes, values are the column names in PostgreSQL.
ACS_VARIABLES: dict[str, str] = {
    "B19013_001E": "median_household_income",
    "B01003_001E": "total_population",
    "B25064_001E": "median_gross_rent",
    "B25077_001E": "median_home_value",
    "B25003_002E": "owner_occupied_units",
    "B25003_003E": "renter_occupied_units",
    "B25002_002E": "occupied_units",
    "B25002_003E": "vacant_units",
}

# Volusia County ZIP codes.
# Used to filter the national ZCTA response down to our area of interest.
VOLUSIA_ZIPS: set[str] = {
    "32114", "32117", "32118", "32119", "32124", "32125", "32127", "32128",
    "32130", "32132", "32141", "32168", "32169", "32174", "32176", "32180",
    "32190", "32198", "32713", "32720", "32721", "32722", "32723", "32724",
    "32725", "32726", "32728", "32732", "32738", "32744", "32759", "32763",
    "32764", "32771",
}

RAW_TABLE = "raw_census_acs"


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_acs() -> pd.DataFrame:
    """
    Fetch ACS 5-year estimates for all ZCTAs from the Census API.

    The Census API returns a 2D array where the first row is column
    headers and subsequent rows are data. This differs from ArcGIS
    which returns a features list — hence a separate client rather
    than reusing ArcGISClient.

    Returns
    -------
    pd.DataFrame
        All ZCTA rows with renamed columns, filtered to Volusia County.

    Raises
    ------
    requests.HTTPError
        If the Census API returns a non-200 response.
    ValueError
        If the response cannot be parsed as expected.
    """
    variables = ",".join(["NAME"] + list(ACS_VARIABLES.keys()))

    params = {
        "get": variables,
        "for": "zip code tabulation area:*",
        "key": CENSUS_API_KEY,
    }

    logger.info("Fetching ACS 5-year estimates from %s", Endpoints.CENSUS_ACS)
    logger.info("Variables: %s", list(ACS_VARIABLES.values()))

    response = requests.get(Endpoints.CENSUS_ACS, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list) or len(data) < 2:
        raise ValueError(
            f"Unexpected Census API response format. "
            f"Expected list of lists, got: {type(data)}"
        )

    headers = data[0]
    rows    = data[1:]
    logger.info("Fetched %s ZCTAs nationally", f"{len(rows):,}")

    df = pd.DataFrame(rows, columns=headers)

    # Extract clean ZIP code from "ZCTA5 XXXXX" format.
    df["zip_code"] = df["zip code tabulation area"].str.strip()

    # Filter to Volusia County ZIPs only.
    df = df[df["zip_code"].isin(VOLUSIA_ZIPS)].copy()
    logger.info("Filtered to %s Volusia County ZCTAs", len(df))

    if df.empty:
        raise ValueError(
            "No Volusia County ZIPs found in Census response. "
            "Check VOLUSIA_ZIPS set or Census API geography."
        )

    # Rename Census variable codes to readable column names.
    rename_map = {**ACS_VARIABLES, "NAME": "zcta_name"}
    df = df.rename(columns=rename_map)

    # Drop the raw geography column — replaced by zip_code.
    df = df.drop(columns=["zip code tabulation area"], errors="ignore")

    # Convert numeric columns — Census returns all values as strings.
    # Sentinel value -666666666 means data not available — convert to null.
    numeric_cols = list(ACS_VARIABLES.values())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where(df[col] != -666666666, other=None)

    # Add ACS vintage year so dbt models know which release this is.
    # Update this when pulling a newer ACS release.
    df["acs_year"] = 2024

    return df


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def add_ingestion_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"]      = "census_acs5_2024"
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("Census ACS ingestion started")
    logger.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("Target: %s.%s", settings.schema, RAW_TABLE)
    logger.info("=" * 60)

    # Step 1: Fetch
    logger.info("Step 1/3 — Fetching ACS data")
    df = fetch_acs()
    df = add_ingestion_metadata(df)

    logger.info("Data prepared — %s rows x %s columns", len(df), len(df.columns))
    logger.info("ZIPs loaded: %s", ", ".join(df["zip_code"].tolist()))

    if dry_run:
        logger.info("DRY RUN — skipping database write.")
        logger.info("Dry run complete.")
        return

    # Step 2: Load
    logger.info("Step 2/3 — Loading into PostgreSQL")
    engine = create_engine(settings.sqlalchemy_url)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.schema}"))
        conn.commit()

    inspector = inspect(engine)

    exists = inspector.has_table(
        RAW_TABLE,
        schema=settings.schema,
    )

    if exists:
        logger.info(
            "Table exists — truncating %s.%s",
            settings.schema,
            RAW_TABLE,
        )

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"TRUNCATE TABLE "
                    f"{settings.schema}.{RAW_TABLE}"
                )
            )

        if_exists = "append"

    else:
        logger.info(
            "Table does not exist — creating %s.%s",
            settings.schema,
            RAW_TABLE,
        )

        if_exists = "replace"

    df.to_sql(
        name=RAW_TABLE,
        con=engine,
        schema=settings.schema,
        if_exists=if_exists,
        index=False,
        chunksize=Pipeline.DB_CHUNKSIZE,
        method="multi",
    )

    # Step 3: Verify
    with engine.connect() as conn:
        rows_loaded = conn.execute(
            text(f"SELECT COUNT(*) FROM {settings.schema}.{RAW_TABLE}")
        ).scalar()

    logger.info("=" * 60)
    logger.info("Census ACS ingestion complete")
    logger.info("  Rows loaded : %s", rows_loaded)
    logger.info("  Table       : %s.%s", settings.schema, RAW_TABLE)
    logger.info("=" * 60)

    engine.dispose()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Census ACS 5-year estimates for Volusia County ZIPs."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Fetch data without writing to the database.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run)
