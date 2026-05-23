"""
config.py
---------
Central configuration for the Volusia Housing Pipeline.

All environment variables, endpoint URLs, and project-wide constants
are defined here. No other file in the ingestion layer should contain
raw credentials, URLs, or magic strings — import them from this module.

Environment variables are loaded from a .env file at the repo root
when running locally. In GitHub Actions, they are injected as secrets
and available as real environment variables — python-dotenv handles
both cases transparently.

Usage
-----
from config import settings, Endpoints

print(settings.db_host)
print(Endpoints.PARCELS)
"""

import logging
import logging.config
import os
from dataclasses import dataclass

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env
# Calling load_dotenv() before anything else ensures os.getenv() calls below
# see the values from .env when running locally. In CI, real environment
# variables take precedence over .env values automatically.
# ---------------------------------------------------------------------------
load_dotenv()


# ---------------------------------------------------------------------------
# Logging
# Configure once here so every module that does logging.getLogger() shares
# the same format without each file needing its own basicConfig() call.
# ---------------------------------------------------------------------------
logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            # Example output:
            # 2024-11-15 22:31:05 | INFO     | volusia.ingestion | Starting paginated fetch — source: parcels
            "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {
        # volusia.* catches all loggers in this project:
        # volusia.ingestion, volusia.pipeline, etc.
        "volusia": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        }
    },
})

logger = logging.getLogger("volusia.config")


# ---------------------------------------------------------------------------
# DatabaseSettings
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DatabaseSettings:
    """
    Immutable container for PostgreSQL connection parameters.

    Loaded from environment variables at import time. If a required
    variable is missing, we fail loudly at startup rather than at
    the point of first use — easier to debug in CI.

    Attributes
    ----------
    host : str
        Supabase pooler hostname.
    port : int
        PostgreSQL port. Default 5432.
    name : str
        Database name. Always 'postgres' on Supabase free tier.
    user : str
        Database user. On Supabase this includes the project ref:
        'postgres.wfnevsqwzdyzwqrtqfym'
    password : str
        Database password.
    schema : str
        Schema where raw ingestion tables are written. Default 'raw'.
    """

    host: str
    port: int
    name: str
    user: str
    password: str
    schema: str

    @property
    def sqlalchemy_url(self) -> str:
        """
        SQLAlchemy connection URL for use with pandas.to_sql() and
        direct psycopg2 connections.

        Returns
        -------
        str
            Format: postgresql+psycopg2://user:password@host:port/dbname
        """
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
            f"?sslmode=require"
        )

    @property
    def psycopg2_kwargs(self) -> dict:
        """
        Keyword arguments for a direct psycopg2.connect() call.
        Used when SQLAlchemy is overkill (e.g. simple schema creation).

        Returns
        -------
        dict
            Connection parameters as keyword arguments.
        """
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.name,
            "user": self.user,
            "password": self.password,
            "sslmode": "require",
        }


def _require_env(key: str) -> str:
    """
    Return the value of an environment variable or raise clearly if missing.

    Parameters
    ----------
    key : str
        Environment variable name.

    Returns
    -------
    str
        The variable's value.

    Raises
    ------
    EnvironmentError
        If the variable is not set or is an empty string.
    """
    value = os.getenv(key, "").strip()
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Add it to your .env file or GitHub Actions secrets."
        )
    return value


# Instantiate once at import time. All modules import this object.
settings = DatabaseSettings(
    host=_require_env("DB_HOST"),
    port=int(os.getenv("DB_PORT", "5432")),
    name=_require_env("DB_NAME"),
    user=_require_env("DB_USER"),
    password=_require_env("DB_PASSWORD"),
    schema=os.getenv("DB_SCHEMA", "raw"),
)

logger.info("Database settings loaded — host: %s | schema: %s", settings.host, settings.schema)


# ---------------------------------------------------------------------------
# ArcGIS Endpoints
# ---------------------------------------------------------------------------

class Endpoints:
    """
    Volusia County ArcGIS FeatureServer endpoint URLs.

    These are the base layer URLs — the ArcGISClient appends /query
    before making requests. Defined as class attributes (not an Enum)
    so they can be used as plain strings without .value unwrapping.

    Notes
    -----
    All endpoints are on the county's public ArcGIS server at
    maps1.vcgov.org. No authentication is required. The server
    enforces a 1,000 record per request cap — the ArcGISClient
    handles pagination automatically.

    If any endpoint returns a 404 or ArcGIS error code 400, the
    county may have restructured their services. Use fetch_fields()
    on the base service URL to rediscover the layer index.
    """

    # Parcel polygons with ownership, valuation, and exemption data.
    # ~300,000 records. Primary source for the owner classification model.
    PARCELS = (
        "https://maps1.vcgov.org/arcgis/rest/services"
        "/CRAPublic/Parcels_Public_WM/FeatureServer/0"
    )

    # Building permits from the AMANDA permit management system.
    # Covers unincorporated Volusia County only — NOT City of Daytona Beach.
    # Updated continuously. Use upsert strategy when loading.
    PERMITS = (
        "https://maps1.vcgov.org/arcgis/rest/services"
        "/GRMs_AMANDA_OPEN_Permits/FeatureServer/0"
    )

    # Countywide zoning boundaries.
    # Supplementary source — used to enrich parcel records with zoning context.
    ZONING = (
        "https://maps1.vcgov.org/arcgis/rest/services"
        "/CountywideZoning/MapServer/0"
    )

    # U.S. Census Bureau ACS 5-Year Estimates API.
    # Variables: median household income (B19013_001E), total population (B01003_001E)
    # Geography: zip code tabulation areas (ZCTA) in Florida (state FIPS 12).
    # No API key required for low-volume requests. Register at api.census.gov
    # for a free key if you hit the 500 req/day anonymous limit.
    CENSUS_ACS = "https://api.census.gov/data/2023/acs/acs5"


# ---------------------------------------------------------------------------
# Pipeline constants
# ---------------------------------------------------------------------------

class Pipeline:
    """
    Project-wide pipeline constants.

    Centralising these here means a single edit propagates to all
    ingestion scripts — no hunting across files for magic numbers.
    """

    # First year of historical tax roll data available from VCPA.
    # Used to drive the multi-year backfill loop in ingest_tax_roll.py.
    TAX_ROLL_START_YEAR: int = 2013

    # Raw table names in PostgreSQL. dbt staging models reference these.
    # Changing a table name here automatically updates all downstream references
    # once the ingestion script is re-run.
    RAW_TABLE_PARCELS: str = "raw_parcels"
    RAW_TABLE_PERMITS: str = "raw_permits"
    RAW_TABLE_TAX_ROLL: str = "raw_tax_roll"

    # ArcGIS client defaults. Override by passing kwargs to ArcGISClient().
    ARCGIS_REQUEST_DELAY: float = 0.5   # seconds between paginated requests
    ARCGIS_TIMEOUT: int = 30            # per-request timeout in seconds
    ARCGIS_MAX_RETRIES: int = 3         # retries before raising

    # pandas.to_sql() write behavior.
    # 'append' is the default for incremental runs.
    # Use 'replace' only during a full historical backfill.
    DB_IF_EXISTS_APPEND: str = "append"
    DB_IF_EXISTS_REPLACE: str = "replace"

    # Chunk size for pandas.to_sql() — number of rows per INSERT batch.
    # 500 is a safe default for Supabase free tier connection pooling.
    DB_CHUNKSIZE: int = 500
