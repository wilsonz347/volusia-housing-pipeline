"""
ingest_tax_roll.py
------------------
Downloads the Volusia County Property Appraiser CAMA database,
converts it from Microsoft Access (.accdb) to CSV using mdbtools,
and loads five slim analytical tables into PostgreSQL.

What this script does
---------------------
1. Downloads CAMA_DATA_EXPORT.zip from vcpa.vcgov.org (~230MB).
2. Extracts the .accdb file to a local temp directory.
3. Exports five tables to CSV via mdb-export, applying column
   selection and row filters at read time.
4. Loads each slimmed table into the raw schema in PostgreSQL.
5. Cleans up all temp files regardless of success or failure.

Tables and filters
------------------
VCPA_CAMA_PARCEL      → raw_cama_parcel      | 12 cols | no row filter
VCPA_CAMA_OWNER       → raw_cama_owner       | 10 cols | OWNSEQ = 0 (primary owner)
VCPA_CAMA_SALES       → raw_cama_sales       |  6 cols | STEB = "Q" (arm's-length only)
VCPA_CAMA_SITUS       → raw_cama_situs       |  4 cols | OWNSEQ = 1 (primary address)
VCPA_CAMA_EXEMPTIONS  → raw_cama_exemptions  |  5 cols | relevant codes only

Column and row filtering rationale
-----------------------------------
Only columns that directly drive the housing pressure analysis are
retained. Descriptions (_DESC fields) are replaced by dbt seed lookup
tables (land_use_codes.csv, exemption_codes.csv) which also add
analytical classification columns not present in the raw data.

Row filters remove records that would never appear in analytical output:
- OWNER: secondary owners (OWNSEQ > 0) are not needed for classification
- SALES: unqualified sales (family transfers, foreclosures, LLC
  restructurings) distort price trend analysis
- EXEMPTIONS: administrative and commercial exemption codes not
  relevant to residential housing analysis

Database contents
-----------------
The CAMA database contains the CURRENT working tax roll only.
All records have TAXYR = 2026. Prior years require a separate
request to the Florida Department of Revenue:
https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx

Load strategy
-------------
TRUNCATE + INSERT.

The CAMA database is a full current-state snapshot with no incremental updates.
Each weekly run replaces all rows for the current assessment year.

Prerequisites
-------------
mdbtools must be installed: brew install mdbtools
Verify: mdb-export --help

Run modes
---------
Full load:   python ingest_tax_roll.py
Dry run:     python ingest_tax_roll.py --dry-run
Local file:  python ingest_tax_roll.py --accdb-path /path/to/file.accdb
"""

import argparse
import io
import logging
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, inspect

from config import Pipeline, settings

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("volusia.ingestion")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CAMA_DOWNLOAD_URL = Pipeline.CAMA_DOWNLOAD_URL
ACCDB_FILENAME    = Pipeline.CAMA_ACCDB_FILENAME

# Exemption codes relevant to residential housing analysis.
# Administrative, commercial, and agricultural codes are excluded.
RELEVANT_EXEMPTION_CODES = {
    "01",     # Standard homestead $25k
    "02",     # Additional homestead $25k (value $50k-$75k)
    "AH",     # Additional homestead 196.031(1)(b)
    "AHC",    # Additional homestead (copy)
    "10CAP",  # 10% non-homestead assessment cap — investor signal
    "65",     # Senior low income exemption
    "TP",     # Total & permanent disability
    "06",     # Veteran exemption
    "07",     # Veteran exemption (disabled)
}

# ---------------------------------------------------------------------------
# Table definitions
# Each entry defines:
#   cama_table  : source table name in the .accdb
#   pg_table    : target PostgreSQL table name
#   columns     : columns to keep (None = keep all)
#   row_filter  : callable applied to DataFrame after export (None = keep all)
# ---------------------------------------------------------------------------

def _owner_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep primary owner only (OWNSEQ = 0)."""
    return df[df["OWNSEQ"] == 0].copy()

def _sales_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep qualified arm's-length sales only (STEB = 'Q')."""
    return df[df["STEB"] == "Q"].copy()

def _situs_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep primary situs address only (OWNSEQ = 1)."""
    return df[df["OWNSEQ"] == 1].copy()

def _exemptions_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only analytically relevant exemption codes."""
    return df[df["EXCODE"].isin(RELEVANT_EXEMPTION_CODES)].copy()


TABLE_DEFINITIONS: list[dict] = [
    {
        "cama_table": "VCPA_CAMA_PARCEL",
        "pg_table":   Pipeline.RAW_TABLE_CAMA_PARCEL,
        "columns": [
            "PARID",        # join key
            "TAXYR",        # assessment year
            "LUC",          # land use code — residential filter in dbt
            "LUC_DESC",     # land use description
            "APRTOT",       # total just value — primary valuation metric
            "SASD",         # school assessed value — SOH differential
            "NSASD",        # non-school assessed value
            "STXBL",        # school taxable value
            "NSTXBL",       # non-school taxable value
            "HX_FLAG",      # homestead flag — owner classification anchor
            "LIVUNIT",      # living units — SF vs multi-family
            "NBHD",         # neighborhood code
            "NBHD_DESC",    # neighborhood description
            "TAXDIST",      # tax district code
            "TAXDIST_DESC", # tax district description
        ],
        "row_filter": None,
    },
    {
        "cama_table": "VCPA_CAMA_OWNER",
        "pg_table":   Pipeline.RAW_TABLE_CAMA_OWNER,
        "columns": [
            "PARID",         # join key
            "TAXYR",         # assessment year
            "OWNSEQ",        # ownership sequence — filter to 0 (primary)
            "OWN1",          # primary owner name
            "OWN2",          # secondary owner name — LLC detection
            "ADDR1",         # mailing address line 1
            "ADDR2",         # mailing address line 2
            "ADDR3",         # city + state — out-of-state detection
            "COUNTRY",       # foreign investor flag
            "OWNTYPE1",      # ownership type code
            "OWNTYPE1_DESC", # ownership type description
        ],
        "row_filter": _owner_filter,
    },
    {
        "cama_table": "VCPA_CAMA_SALES",
        "pg_table":   Pipeline.RAW_TABLE_CAMA_SALES,
        "columns": [
            "PARID",   # join key
            "TAXYR",   # assessment year
            "SALEDT",  # sale date
            "PRICE",   # sale price — market value signal
            "STEB",    # qualification code — always "Q" after filter
            "APRTOT",  # appraised value at time of sale
        ],
        "row_filter": _sales_filter,
    },
    {
        "cama_table": "VCPA_CAMA_SITUS",
        "pg_table":   Pipeline.RAW_TABLE_CAMA_SITUS,
        "columns": [
            "PARID",    # join key
            "TAXYR",    # assessment year
            "OWNSEQ",   # sequence — filter to 1 (primary address)
            "CITYNAME", # physical city
            "ZIP1",     # physical ZIP — primary geographic aggregation unit
        ],
        "row_filter": _situs_filter,
    },
    {
        "cama_table": "VCPA_CAMA_EXEMPTIONS",
        "pg_table":   Pipeline.RAW_TABLE_CAMA_EXEMPTIONS,
        "columns": [
            "PARID",       # join key
            "TAXYR",       # assessment year
            "EXCODE",      # exemption code
            "EXCODE_DESC", # human readable
            "YRBEG",       # year exemption began — tenure signal
        ],
        "row_filter": _exemptions_filter,
    },
]


# ---------------------------------------------------------------------------
# Prerequisite check
# ---------------------------------------------------------------------------

def check_mdbtools() -> None:
    """
    Confirm mdbtools is installed and accessible on PATH.

    Raises
    ------
    EnvironmentError
        If mdb-export is not found.
    """
    if shutil.which("mdb-export") is None:
        raise EnvironmentError(
            "mdbtools is not installed or not on PATH. "
            "Install with: brew install mdbtools"
        )
    logger.info("mdbtools confirmed available")


# ---------------------------------------------------------------------------
# Download and extract
# ---------------------------------------------------------------------------

def download_cama(dest_dir: Path) -> Path:
    """
    Download CAMA_DATA_EXPORT.zip from the VCPA website.

    Parameters
    ----------
    dest_dir : Path
        Directory to save the downloaded zip file.

    Returns
    -------
    Path
        Path to the downloaded zip file.
    """
    zip_path = dest_dir / "CAMA_DATA_EXPORT.zip"
    logger.info("Downloading CAMA database (~230MB) — this takes a few minutes...")

    def _log_progress(block_count, block_size, total_size):
        downloaded = block_count * block_size
        if total_size > 0 and block_count % 500 == 0:
            pct = min(downloaded / total_size * 100, 100)
            logger.info("  Download progress: %.1f%%", pct)

    urllib.request.urlretrieve(CAMA_DOWNLOAD_URL, zip_path, reporthook=_log_progress)
    size_mb = zip_path.stat().st_size / 1_048_576
    logger.info("Download complete — %.1fMB", size_mb)
    return zip_path


def extract_accdb(zip_path: Path, dest_dir: Path) -> Path:
    """
    Extract the .accdb file from the downloaded zip.

    Parameters
    ----------
    zip_path : Path
        Path to the downloaded zip file.
    dest_dir : Path
        Directory to extract into.

    Returns
    -------
    Path
        Path to the extracted .accdb file.

    Raises
    ------
    FileNotFoundError
        If the expected .accdb file is not found after extraction.
    """
    logger.info("Extracting %s", ACCDB_FILENAME)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extract(ACCDB_FILENAME, dest_dir)

    accdb_path = dest_dir / ACCDB_FILENAME
    if not accdb_path.exists():
        raise FileNotFoundError(
            f"Expected {ACCDB_FILENAME} after extraction but not found."
        )
    logger.info("Extracted successfully")
    return accdb_path


# ---------------------------------------------------------------------------
# Export and filter
# ---------------------------------------------------------------------------

def export_table(accdb_path: Path, table_def: dict) -> pd.DataFrame:
    """
    Export a single CAMA table to a filtered DataFrame.

    Applies column selection and row filter defined in table_def.
    Column selection happens after export (pandas loc) rather than
    at the mdb-export level because mdb-export does not support
    column selection natively.

    Parameters
    ----------
    accdb_path : Path
        Path to the .accdb file.
    table_def : dict
        Table definition dict from TABLE_DEFINITIONS.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame ready for metadata addition and load.

    Raises
    ------
    subprocess.CalledProcessError
        If mdb-export exits with a non-zero return code.
    """
    cama_table = table_def["cama_table"]
    columns    = table_def["columns"]
    row_filter = table_def["row_filter"]

    logger.info("Exporting %s", cama_table)

    result = subprocess.run(
        ["mdb-export", str(accdb_path), cama_table],
        capture_output=True,
        text=True,
        check=True,
        encoding="utf-8",
    )

    if not result.stdout.strip():
        logger.warning("mdb-export returned empty output for %s", cama_table)
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO(result.stdout), low_memory=False)
    rows_raw = len(df)

    # Column selection.
    available = [c for c in columns if c in df.columns]
    missing   = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning(
            "%s — expected columns not found: %s",
            cama_table, missing
        )
    df = df[available]

    # Row filter.
    if row_filter is not None:
        df = row_filter(df)
        logger.info(
            "%s — %s rows after filter (was %s)",
            cama_table,
            f"{len(df):,}",
            f"{rows_raw:,}",
        )
    else:
        logger.info(
            "%s — %s rows | %s columns",
            cama_table,
            f"{len(df):,}",
            len(df.columns),
        )

    return df


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def add_ingestion_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Append ingested_at and source metadata columns.

    Parameters
    ----------
    df : pd.DataFrame
        Filtered DataFrame.
    source : str
        Source identifier string.

    Returns
    -------
    pd.DataFrame
        DataFrame with metadata columns appended.
    """
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    df["source"]      = source
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase all column names for PostgreSQL compatibility."""
    df.columns = df.columns.str.lower()
    return df


# ---------------------------------------------------------------------------
# Database load
# ---------------------------------------------------------------------------

def load_table(df: pd.DataFrame, pg_table: str, engine) -> None:
    """
    Load a DataFrame into PostgreSQL using TRUNCATE + INSERT strategy.

    Parameters
    ----------
    df : pd.DataFrame
        Prepared DataFrame.
    pg_table : str
        Target PostgreSQL table name.
    engine : sqlalchemy.Engine
        Active SQLAlchemy engine.
    """
        
    logger.info(
        "Loading %s rows into %s.%s",
        f"{len(df):,}",
        settings.schema,
        pg_table,
    )

    inspector = inspect(engine)

    exists = inspector.has_table(
        pg_table,
        schema=settings.schema,
    )

    if exists:
        logger.info(
            "Table exists — truncating %s.%s",
            settings.schema,
            pg_table,
        )

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"TRUNCATE TABLE {settings.schema}.{pg_table}"
                )
            )

        if_exists = "append"

    else:
        logger.info(
            "Table does not exist — creating %s.%s",
            settings.schema,
            pg_table,
        )

        if_exists = "replace"

    df.to_sql(
        name=pg_table,
        con=engine,
        schema=settings.schema,
        if_exists=if_exists,
        index=False,
        chunksize=Pipeline.DB_CHUNKSIZE,
        method="multi",
    )

    with engine.connect() as conn:
        rows_loaded = conn.execute(
            text(
                f"SELECT COUNT(*) "
                f"FROM {settings.schema}.{pg_table}"
            )
        ).scalar()

    logger.info(
        "Verified %s rows in %s.%s",
        f"{rows_loaded:,}",
        settings.schema,
        pg_table,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool = False, accdb_path: str | None = None) -> None:
    """
    Execute the full CAMA tax roll ingestion pipeline.

    Parameters
    ----------
    dry_run : bool
        If True, downloads and converts but does not write to the database.
    accdb_path : str, optional
        Path to a locally available .accdb file. If provided, skips
        the download step. Useful for testing without re-downloading.
    """
    logger.info("=" * 60)
    logger.info("CAMA tax roll ingestion started")
    logger.info("Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Prerequisites
    # ------------------------------------------------------------------
    logger.info("Step 1/4 — Checking prerequisites")
    check_mdbtools()

    tmp_dir = None

    try:
        # ------------------------------------------------------------------
        # Step 2: Acquire .accdb file
        # ------------------------------------------------------------------
        if accdb_path:
            # Use locally provided file — skip download.
            accdb = Path(accdb_path)
            if not accdb.exists():
                logger.error("Provided --accdb-path does not exist: %s", accdb_path)
                sys.exit(1)
            logger.info("Step 2/4 — Using local file: %s", accdb)
        else:
            tmp_dir = Path(tempfile.mkdtemp(prefix="volusia_cama_"))
            logger.info("Step 2/4 — Downloading and extracting CAMA database")
            zip_path = download_cama(tmp_dir)
            accdb    = extract_accdb(zip_path, tmp_dir)

        # ------------------------------------------------------------------
        # Step 3: Export, filter, and prepare all tables
        # ------------------------------------------------------------------
        logger.info("Step 3/4 — Exporting and filtering tables")
        prepared: list[tuple[str, pd.DataFrame]] = []

        for table_def in TABLE_DEFINITIONS:
            df = export_table(accdb, table_def)

            if df.empty:
                logger.warning("Skipping %s — empty after export/filter", table_def["cama_table"])
                continue

            df = normalize_column_names(df)
            df = add_ingestion_metadata(df, f"vcpa_cama_{table_def['pg_table']}")
            prepared.append((table_def["pg_table"], df))

        if dry_run:
            logger.info("DRY RUN — skipping all database writes.")
            for pg_table, df in prepared:
                logger.info(
                    "  Would load %s rows x %s cols into %s.%s",
                    f"{len(df):,}", len(df.columns),
                    settings.schema, pg_table,
                )
            logger.info("Dry run complete.")
            return

        # ------------------------------------------------------------------
        # Step 4: Load to PostgreSQL
        # ------------------------------------------------------------------
        logger.info("Step 4/4 — Loading into PostgreSQL")
        engine = create_engine(settings.sqlalchemy_url)

        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.schema}"))
            conn.commit()

        for pg_table, df in prepared:
            load_table(df, pg_table, engine)

        engine.dispose()

        # Summary
        logger.info("=" * 60)
        logger.info("CAMA ingestion complete")
        for pg_table, df in prepared:
            logger.info(
                "  %s.%s — %s rows",
                settings.schema, pg_table, f"{len(df):,}",
            )
        logger.info("=" * 60)

    finally:
        if tmp_dir:
            logger.info("Cleaning up temp directory")
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download and ingest Volusia County CAMA tax roll. "
            "Requires mdbtools: brew install mdbtools"
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Export and filter without writing to the database.",
    )
    parser.add_argument(
        "--accdb-path",
        type=str,
        default=None,
        help=(
            "Path to a locally available .accdb file. "
            "Skips the download step. "
            "Example: --accdb-path /tmp/cama_test/CAMA_DATA_EXPORT_WEB.accdb"
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run, accdb_path=args.accdb_path)
