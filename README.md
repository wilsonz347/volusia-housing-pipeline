# Volusia County Housing Intelligence Pipeline

An end-to-end data engineering pipeline that ingests, models, and visualizes public government data to analyze housing displacement pressure in Volusia County, Florida.

**Core question:** Where is absentee investor ownership of residential property accelerating — and what measurable pressure is it exerting on housing affordability for long-term residents?

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Repository Structure](#repository-structure)
- [Setup](#setup)
- [Ingestion Layer](#ingestion-layer)
- [Transformation Layer](#transformation-layer)
- [Orchestration](#orchestration)
- [Known Limitations](#known-limitations)

---

## Project Overview

This project integrates property assessment, permitting, demographic, and public health datasets to measure housing displacement pressure across Volusia County, Florida. The primary focus is identifying concentrations of absentee investor ownership and their relationship to affordability and neighborhood outcomes.

**Intended consumers:** One Voice for Volusia, Daytona Beach News-Journal, local housing nonprofits, and city planners.

**Stack:** Python · PostgreSQL (Supabase) · dbt Core · GitHub Actions · Data Studio

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  INGESTION (Python)                                         │
│                                                             │
│  VCPA CAMA .accdb  →  raw_cama_parcel                       │
│                    →  raw_cama_owner                        │
│                    →  raw_cama_sales                        │
│                    →  raw_cama_situs                        │
│                    →  raw_cama_exemptions                   │
│                                                             │
│  ArcGIS MapServer  →  raw_permits                           │
│  ArcGIS MapServer  →  raw_overdose_analysis                 │
│  Census ACS API    →  raw_census_acs                        │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  STORAGE (PostgreSQL — Supabase)                            │
│  Schema: raw  →  staging  →  intermediate  →  marts         │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORMATION (dbt Core)                                  │
│                                                             │
│  staging/        — clean and standardize each source        │
│  intermediate/   — join, enrich, classify                   │
│  marts/          — ZIP-level analytical outputs             │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION (GitHub Actions)                                   │
│  Weekly cron → ingest → dbt run → dbt test → Data Studio refresh  │
└───────────────────────────┬───────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  SERVING (Data Studio Service)                              │
│  Public dashboard — ZIP-level housing pressure metrics      │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Table | Rows | Cadence | Access |
|---|---|---|---|---|
| VCPA CAMA Database | `raw_cama_parcel` | ~346k | Weekly download | vcpa.vcgov.org |
| VCPA CAMA Database | `raw_cama_owner` | ~344k | Weekly download | vcpa.vcgov.org |
| VCPA CAMA Database | `raw_cama_sales` | ~482k | Weekly download | vcpa.vcgov.org |
| VCPA CAMA Database | `raw_cama_situs` | ~367k | Weekly download | vcpa.vcgov.org |
| VCPA CAMA Database | `raw_cama_exemptions` | ~684k | Weekly download | vcpa.vcgov.org |
| AMANDA Permits (ArcGIS) | `raw_permits` | ~3k | Weekly API | maps5.vcgov.org |
| VSO Overdose Analysis (ArcGIS) | `raw_overdose_analysis` | ~30 | Weekly API | maps5.vcgov.org |
| U.S. Census ACS 2024 | `raw_census_acs` | 29 ZIPs | Annual | api.census.gov |

**Geographic scope:** Volusia County, Florida. AMANDA permits cover unincorporated areas only — City of Daytona Beach is excluded.

**CAMA data note:** The weekly CAMA download contains the current assessment year only (2026). Historical rolls (2020-2025) require a separate Florida DOR data request. See [Florida DOR Data Portal](https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx).

---

## Repository Structure
```
volusia-housing-pipeline/
├── ingestion/
│   ├── config.py                  # DB settings, endpoints, pipeline constants
│   ├── arcgis_client.py           # Paginated ArcGIS FeatureServer/MapServer client
│   ├── ingest_tax_roll.py         # CAMA .accdb download → 5 PostgreSQL tables
│   ├── ingest_permits.py          # AMANDA permits via ArcGIS (UPSERT on folderrsn)
│   ├── ingest_census.py           # Census ACS 5-year via api.census.gov
│   └── ingest_overdose.py         # VSO overdose analysis via ArcGIS
├── volusia_housing/               # dbt project
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql   # Prevents schema prefix doubling
│   ├── seeds/
│   │   └── permit_types.csv       # Permit type lookup
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
└── .github/
    └── workflows/
        └── pipeline.yml           # Weekly CI/CD
```

---

## Setup

### Prerequisites

- Python 3.11+
- dbt-postgres (`pip install dbt-postgres`)
- mdbtools (`brew install mdbtools`) — required for CAMA ingestion
- Supabase account (free tier) — [supabase.com](https://supabase.com)
- Census API key — [api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html)

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/volusia-housing-pipeline
cd volusia-housing-pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

Create a `.env` file in the repo root:

```env
DB_HOST=aws-1-us-east-2.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.your_project_ref
DB_PASSWORD=your_password
DB_SCHEMA=raw
CENSUS_API_KEY=your_census_key
```

### dbt profile

Add to `~/.dbt/profiles.yml`:

```yaml
volusia_housing:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST') }}"
      port: 5432
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') }}"
      dbname: "{{ env_var('DB_NAME') }}"
      schema: dev
      threads: 4
      sslmode: require
```

Verify connection:

```bash
cd volusia_housing
dbt debug
```

---

## Ingestion Layer

All scripts support `--dry-run` to fetch and validate without writing to the database. Always run dry first.

```bash
cd ingestion

# Dry runs
python ingest_tax_roll.py --dry-run
python ingest_permits.py --dry-run
python ingest_census.py --dry-run
python ingest_overdose.py --dry-run

# Live loads
python ingest_tax_roll.py        # ~15 min — downloads 230MB CAMA file
python ingest_permits.py         # ~30 sec
python ingest_census.py          # ~5 sec
python ingest_overdose.py        # ~5 sec
```

---

## Transformation Layer

dbt models are organized in three layers:

**Staging** (`models/staging/`) — one model per raw table. Responsibilities: column renaming, type casting, null handling, basic validation. Materialized as views.

**Intermediate** (`models/intermediate/`) — cross-source joins and derived fields. Key models:

- `int_owner_classification` — classifies each parcel owner as `owner_occupied`, `out_of_state_investor`, `foreign_investor`, `corporate_investor_fl`, `trust`, `local_investor_individual`, or `unclassified`. Uses homestead flag, mailing state parsed from `addr3`, and LLC keyword matching on owner name.

- `int_value_trends` — computes SOH differential (just value minus assessed value) per parcel. Quantifies displacement risk for long-term homeowners.

- `int_overdose_by_zip` — joins overdose incident data with ACS demographic data at the ZIP code level. Adds population-normalized metrics including `overdose_rate_per_1000` and `opioid_rate_per_1000`, along with socioeconomic context such as median household income, renter occupancy ratio, and rent burden. Serves as the analytical foundation for examining relationships between housing conditions, investor activity, and overdose prevalence.

**Marts** (`models/marts/`) — final analytical outputs consumed by Data Studio. Materialized as tables.

- `mart_housing_pressure` — by ZIP: investor ownership %, SOH differential, median just value, permit activity rate, overdose rate, Census income/rent context.
- `mart_permit_velocity` — by neighborhood: median permit approval time, permit type breakdown.

Run dbt:

```bash
cd volusia_housing
dbt seed          # load lookup tables
dbt run           # build all models
dbt test          # validate data quality
dbt docs generate # generate lineage documentation
dbt docs serve    # view in browser
```

---

## Orchestration

GitHub Actions runs the full pipeline on a weekly cron schedule every Monday at 06:00 UTC.

```
ingest (all four scripts) → dbt run → dbt test → Data Studio dataset refresh
```

**GitHub Secrets required:**

```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, CENSUS_API_KEY
```

---

## Known Limitations

**CAMA is current-year only.** The weekly download contains 2026 assessment data only. Multi-year trend analysis requires historical rolls from the Florida DOR data portal (free request, ~3 business days turnaround).

**AMANDA permits exclude incorporated cities.** The permits layer covers unincorporated Volusia County, Deltona, DeBary, and Pierson. City of Daytona Beach, Ormond Beach, and other municipalities issue permits through separate systems not included here.

**Owner classification is probabilistic.** The `int_owner_classification` model derives investor status from observable signals — homestead flag, mailing state, entity name patterns. Some seasonal residents are misclassified as out-of-state investors. Coastal ZIPs (32118, 32127) are particularly affected. Low-confidence classifications are excluded from headline metrics.

**Mailing state requires parsing.** CAMA `addr3` stores city and state as free text (`"DAYTONA BEACH FL"`). State extraction uses string splitting — edge cases exist for non-standard formats and foreign addresses.

**Census ACS is a 5-year average.** The 2024 ACS release covers 2020-2024. It does not reflect 2025-2026 conditions and should be treated as structural neighborhood context rather than current-year data.
