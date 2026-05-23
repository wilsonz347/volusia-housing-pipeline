"""
arcgis_client.py
----------------
Reusable paginated client for Volusia County ArcGIS FeatureServer endpoints.

All county datasets are served through the standard ESRI ArcGIS REST API.
This client handles the three constraints common to all county endpoints:

  1. Max 1,000 records per request  — handled via offset pagination
  2. No documented rate limit       — handled via configurable request delay
  3. Dates returned as Unix ms      — handled via optional timestamp conversion

Usage
-----
from arcgis_client import ArcGISClient

client = ArcGISClient()
df = client.fetch_all(
    endpoint="https://maps1.vcgov.org/arcgis/rest/services/GRMs_AMANDA_OPEN_Permits/FeatureServer/0",
    label="permits",
)

Design decisions
----------------
- Returns a pandas DataFrame so callers can immediately inspect or load to PostgreSQL.
- Stateless: no session state is held between calls. Instantiate once, call many times.
- Pagination uses resultOffset / resultRecordCount, which is supported by all
  ArcGIS FeatureServer endpoints version 10.3+.
- A small inter-request delay (default 0.5s) prevents hammering a government
  server that has no documented rate limit — this is intentional courtesy.
"""

import logging
import time
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Module-level logger
# All ingestion scripts share this logger name so log output is consistent.
# ---------------------------------------------------------------------------
logger = logging.getLogger("volusia.ingestion")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Standard ArcGIS query parameters shared across all county endpoints.
_BASE_QUERY_PARAMS: dict = {
    "where": "1=1",          # Return all records — filtering happens in dbt, not here.
    "outFields": "*",         # Return all fields — we never drop columns at ingestion.
    "returnGeometry": "false",# Geometry (lat/lon polygons) is not needed for tabular analysis.
    "f": "json",              # Response format.
}

# ArcGIS returns dates as Unix milliseconds. Divide by 1,000 to get Unix seconds,
# then convert to a pandas Timestamp. Fields matching this suffix pattern are dates.
_DATE_FIELD_SUFFIXES = ("_DATE", "_DT", "DATE", "TIMESTAMP")

# Default page size. The Volusia County FeatureServer caps at 1,000.
# Setting this lower than the cap is safe but slower.
_PAGE_SIZE = 1_000


# ---------------------------------------------------------------------------
# ArcGISClient
# ---------------------------------------------------------------------------

class ArcGISClient:
    """
    Paginated client for ArcGIS FeatureServer REST endpoints.

    Parameters
    ----------
    request_delay : float
        Seconds to wait between paginated requests. Default 0.5.
        Increase if you observe HTTP 429 or connection resets.
    timeout : int
        Per-request timeout in seconds. Default 30.
    max_retries : int
        Number of times to retry a failed request before raising.
        Uses exponential backoff. Default 3.
    """

    def __init__(
        self,
        request_delay: float = 0.5,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        self.request_delay = request_delay
        self.timeout = timeout
        self._session = self._build_session(max_retries)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_all(
        self,
        endpoint: str,
        label: str,
        extra_params: Optional[dict] = None,
        convert_timestamps: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch every record from a FeatureServer layer via offset pagination.

        Parameters
        ----------
        endpoint : str
            Full URL to the FeatureServer layer, without /query.
            Example: "https://maps1.vcgov.org/.../FeatureServer/0"
        label : str
            Human-readable name used in log messages (e.g. "parcels", "permits").
        extra_params : dict, optional
            Additional query parameters to merge into every request.
            Useful for server-side filtering, e.g. {"where": "DOR_UC='001'"}.
        convert_timestamps : bool
            If True, converts Unix millisecond fields to pandas Timestamps.
            Default True.

        Returns
        -------
        pd.DataFrame
            All records concatenated into a single DataFrame.
            Returns an empty DataFrame (not raises) if no records are found.

        Raises
        ------
        requests.HTTPError
            If a request fails after all retries are exhausted.
        ValueError
            If the API response does not contain a 'features' key,
            which usually indicates an authentication error or wrong URL.
        """
        query_url = f"{endpoint.rstrip('/')}/query"
        params = {**_BASE_QUERY_PARAMS, **(extra_params or {})}

        logger.info("Starting paginated fetch — source: %s", label)

        pages: list[list[dict]] = []
        offset = 0

        while True:
            page_params = {
                **params,
                "resultOffset": offset,
                "resultRecordCount": _PAGE_SIZE,
            }

            response = self._get(query_url, page_params)
            data = response.json()

            # Validate the response shape before touching it.
            self._validate_response(data, query_url)

            features = data.get("features", [])
            if not features:
                # Empty page — we've consumed all records.
                break

            records = [f["attributes"] for f in features]
            pages.append(records)

            record_count = offset + len(records)
            logger.info("  %s: fetched %d records so far...", label, record_count)

            # ArcGIS signals the last page when it returns fewer records than
            # the page size. Checking this avoids one extra round-trip.
            if len(records) < _PAGE_SIZE:
                break

            offset += _PAGE_SIZE
            time.sleep(self.request_delay)

        if not pages:
            logger.warning("No records returned for source: %s", label)
            return pd.DataFrame()

        all_records = [record for page in pages for record in page]
        df = pd.DataFrame(all_records)

        logger.info(
            "Completed fetch — source: %s | rows: %d | columns: %d",
            label,
            len(df),
            len(df.columns),
        )

        if convert_timestamps:
            df = self._convert_timestamp_columns(df)

        return df

    def fetch_record_count(self, endpoint: str) -> int:
        """
        Return the total record count for a FeatureServer layer without
        fetching any data. Useful for pre-flight checks and monitoring.

        Parameters
        ----------
        endpoint : str
            Full URL to the FeatureServer layer, without /query.

        Returns
        -------
        int
            Total number of records available. Returns -1 if the count
            cannot be determined (some endpoints do not support this).
        """
        query_url = f"{endpoint.rstrip('/')}/query"
        params = {
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json",
        }

        response = self._get(query_url, params)
        data = response.json()

        count = data.get("count", -1)
        logger.debug("Record count for %s: %s", endpoint, count)
        return count

    def fetch_fields(self, endpoint: str) -> list[dict]:
        """
        Return the field definitions for a FeatureServer layer.
        Useful for confirming field names and types before building
        your dbt staging schema.

        Parameters
        ----------
        endpoint : str
            Full URL to the FeatureServer layer, without /query.

        Returns
        -------
        list[dict]
            List of field definition dicts, each containing at minimum:
            'name', 'type', 'alias', 'length' (where applicable).
        """
        layer_url = endpoint.rstrip("/")
        params = {"f": "json"}

        response = self._get(layer_url, params)
        data = response.json()

        fields = data.get("fields", [])
        logger.debug("Field definitions fetched: %d fields", len(fields))
        return fields

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict) -> requests.Response:
        """
        Execute a GET request with timeout and retry handling.
        Raises requests.HTTPError on non-2xx responses after retries.
        """
        response = self._session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response

    def _validate_response(self, data: dict, url: str) -> None:
        """
        Confirm the response has the expected ArcGIS structure.

        An 'error' key in the response indicates an API-level error
        (e.g. invalid query, authentication required) as opposed to
        an HTTP error, which the session handles separately.
        """
        if "error" in data:
            code = data["error"].get("code", "unknown")
            message = data["error"].get("message", "no message provided")
            raise ValueError(
                f"ArcGIS API error (code {code}) for URL {url}: {message}"
            )

        if "features" not in data:
            raise ValueError(
                f"Unexpected response structure from {url}. "
                f"Expected 'features' key. Got keys: {list(data.keys())}"
            )

    @staticmethod
    def _convert_timestamp_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Unix millisecond integer columns to pandas Timestamps.

        ArcGIS returns all date fields as Unix milliseconds (int64).
        This converts any column whose name ends with a known date suffix
        to a proper datetime, making downstream dbt models cleaner.

        Columns that fail conversion are left unchanged — this is
        intentional: we never want ingestion to silently drop data.
        """
        for col in df.columns:
            col_upper = col.upper()
            is_date_column = any(
                col_upper.endswith(suffix) for suffix in _DATE_FIELD_SUFFIXES
            )

            if not is_date_column:
                continue

            # Only convert if the column is numeric (Unix ms).
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue

            try:
                df[col] = pd.to_datetime(df[col], unit="ms", utc=True)
                logger.debug("Converted timestamp column: %s", col)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Could not convert column %s to timestamp: %s", col, exc
                )

        return df

    @staticmethod
    def _build_session(max_retries: int) -> requests.Session:
        """
        Build a requests Session with automatic retry on transient failures.

        Retries on:
          - HTTP 429 (rate limited — unlikely but possible)
          - HTTP 500, 502, 503, 504 (server-side transient errors)
          - Connection errors and timeouts

        Uses exponential backoff: waits 1s, 2s, 4s between attempts.
        """
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
