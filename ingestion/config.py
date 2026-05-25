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
CENSUS_API_KEY = _require_env("CENSUS_API_KEY")

logger.info("Database settings loaded — host: %s | schema: %s", settings.host, settings.schema)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

class Endpoints:
    """
    Volusia County ArcGIS service endpoint URLs.
    """
    # AMANDA open permits — unincorporated Volusia County only
    PERMITS = (
        "https://maps5.vcgov.org/arcgis/rest/services"
        "/CurrentProjects/MapServer/1"
    )

    # Overdose analysis — ZIP-level counts 2021-2024
    OVERDOSE_ANALYSIS = (
        "https://maps5.vcgov.org/arcgis/rest/services"
        "/OverdoseAnalysis/MapServer/10"
    )

    # Census ACS 5-year estimates — 2024 release
    CENSUS_ACS = "https://api.census.gov/data/2024/acs/acs5"


# ---------------------------------------------------------------------------
# Pipeline constants
# ---------------------------------------------------------------------------

class Pipeline:
    # CAMA download
    CAMA_DOWNLOAD_URL: str = "https://vcpa.vcgov.org/files/database/CAMA_DATA_EXPORT.zip"
    CAMA_ACCDB_FILENAME: str = "CAMA_DATA_EXPORT_WEB.accdb"
    
    # CAMA tables
    RAW_TABLE_CAMA_PARCEL:     str = "raw_cama_parcel"
    RAW_TABLE_CAMA_OWNER:      str = "raw_cama_owner"
    RAW_TABLE_CAMA_SALES:      str = "raw_cama_sales"
    RAW_TABLE_CAMA_SITUS:      str = "raw_cama_situs"
    RAW_TABLE_CAMA_EXEMPTIONS: str = "raw_cama_exemptions"

    # Other sources
    RAW_TABLE_PERMITS:  str = "raw_permits"
    RAW_TABLE_CENSUS:   str = "raw_census_acs"
    RAW_TABLE_OVERDOSE: str = "raw_overdose_analysis"

    # ArcGIS client defaults
    ARCGIS_REQUEST_DELAY: float = 0.5
    ARCGIS_TIMEOUT:       int   = 30
    ARCGIS_MAX_RETRIES:   int   = 3

    # pandas.to_sql() settings
    DB_IF_EXISTS_APPEND:  str = "append"
    DB_IF_EXISTS_REPLACE: str = "replace"
    DB_CHUNKSIZE:         int = 500
