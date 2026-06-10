# Volusia County Housing Intelligence Pipeline

An end-to-end data engineering pipeline that ingests, models, and analyzes public housing, permitting, demographic, and health data to measure housing displacement pressure in Volusia County, Florida.

**Core question:** Where is absentee investor ownership of residential property accelerating — and what measurable pressure is it exerting on housing affordability for long-term residents?

---

## Table of Contents

- [Project Overview](#project-overview)
- [Key Findings](#key-findings)
- [Dashboard Preview](#dashboard-preview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Repository Structure](#repository-structure)
- [Setup](#setup)
- [Ingestion Layer](#ingestion-layer)
- [Transformation Layer](#transformation-layer)
- [Orchestration](#orchestration)
- [Presentation](#presentation)
- [Known Limitations](#known-limitations)

---

## Project Overview

This project integrates property assessment, permitting, demographic, and public health datasets to measure housing displacement pressure across Volusia County, Florida. The primary focus is identifying concentrations of absentee investor ownership and their relationship to affordability and neighborhood outcomes.

**Intended consumers:** One Voice for Volusia, Daytona Beach News-Journal, local housing nonprofits, and city planners.

**Stack:** Python · PostgreSQL (Supabase) · dbt · GitHub Actions · HTML/CSS/JS

---

## Key Findings

- 16.4% of residential parcels are investor-owned countywide (parcel-weighted)
- 9.8% are high-confidence absentee owners (out-of-state or foreign investors)
- 65.3% of residential parcels are owner-occupied based on homestead exemption status
- ZIP 32118 has the highest investor concentration (40.6%) and the highest composite housing pressure score (68.5)
- ZIP 32169 has the largest median Save Our Homes differential ($201K), indicating the highest potential displacement exposure if ownership changes
- Coastal ZIP codes generally exhibit higher investor concentration and larger Save Our Homes protection gaps than inland ZIPs
- Overdose rates declined across nearly all ZIP codes between 2021 and 2024
- Rent burden shows a moderate positive correlation with overdose rates (r = 0.54), while investor ownership shows a weaker positive correlation (r = 0.26)
- Council District 3 has the highest permit stall rate (43.1%), representing more than 394 stalled permits

---

## Dashboard Preview

[View Dashboard Preview](docs/images/volusia-county-presentation.png)

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
┌─────────────────────────────────────────────────────────────┐
│  ORCHESTRATION (GitHub Actions)                             │
│  Weekly cron → ingest → dbt run → dbt test                  │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  SERVING                                                    │
│  Static HTML dashboard — self-contained, repo-hosted        │
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

**CAMA data note:** The weekly CAMA download contains the current assessment year only (2026). Historical rolls (2020–2025) require a separate Florida DOR data request. See [Florida DOR Data Portal](https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx).

---

## Repository Structure

```
volusia-housing-pipeline/
├── ingestion/
│   ├── config.py                      # DB settings, endpoints, pipeline constants
│   ├── arcgis_client.py               # Paginated ArcGIS FeatureServer/MapServer client
│   ├── ingest_tax_roll.py             # CAMA .accdb download → 5 PostgreSQL tables
│   ├── ingest_permits.py              # AMANDA permits via ArcGIS (UPSERT on folderrsn)
│   ├── ingest_census.py               # Census ACS 5-year via api.census.gov
│   └── ingest_overdose.py             # VSO overdose analysis via ArcGIS
├── volusia_housing/                   # dbt project
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql   # Prevents schema prefix doubling
│   ├── seeds/
│   │   └── permit_types.csv           # Permit type lookup
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── dashboard/                         # Static HTML dashboard
│   ├── index.html
│   ├── styles.css
│   ├── app.js
│   └── data/
│       ├── mart_housing_pressure.csv  # Exported from Supabase — public data
│       └── mart_permit_velocity.csv
└── .github/
    └── workflows/
        └── pipeline.yml               # Weekly CI/CD
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
git clone https://github.com/wilsonz347/volusia-housing-pipeline
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
python ingest_tax_roll.py       
python ingest_permits.py        
python ingest_census.py         
python ingest_overdose.py      
```

---

## Transformation Layer

dbt models are organized in three layers:

**Staging** (`models/staging/`) — one model per raw table. Responsibilities: column renaming, type casting, null handling, basic validation. Materialized as views.

**Intermediate** (`models/intermediate/`) — cross-source joins and derived fields. Key models:

- `int_owner_classification` — classifies each parcel as `owner_occupied`, `out_of_state_investor`, `foreign_investor`, `corporate_investor_fl`, `trust`, `local_investor_individual`, or `unclassified`. Uses homestead flag, mailing state parsed from `addr3`, and LLC keyword matching on owner name.
- `int_value_trends` — computes SOH differential (just value minus assessed value) per parcel. Quantifies displacement risk for long-term homeowners.
- `int_overdose_by_zip` — joins overdose incident data with ACS demographic data at ZIP level. Adds population-normalized rates and socioeconomic context for housing-health correlation analysis.

**Marts** (`models/marts/`) — final analytical outputs. Materialized as tables.

- `mart_housing_pressure` — one row per ZIP: investor ownership %, SOH differential, median appraised value, overdose rate, Census income/rent context.
- `mart_permit_velocity` — one row per council district: permit volume, type breakdown, stall rate, median permit age.

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

GitHub Actions runs the full pipeline weekly every Monday at 06:00 UTC.

```
ingest (all four scripts) → dbt run → dbt test
```

**GitHub Secrets required:**

```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, CENSUS_API_KEY
```

---

## Presentation

A static HTML dashboard is included in `dashboard/` and consumes the two mart CSVs directly. It is a read-only presentation layer — no business logic is reimplemented here; all classification and aggregation lives in dbt.

> **Note:** The dashboard template was AI-assisted.

### Running locally

```bash
cd dashboard
python3 -m http.server 8000
# Open http://localhost:8000
```

### Updating data

Export both marts from Supabase and replace the files in `dashboard/data/`:

```
dashboard/data/mart_housing_pressure.csv
dashboard/data/mart_permit_velocity.csv
```

### Views

| Tab | Source | Description |
|---|---|---|
| Executive View | mart_housing_pressure | KPIs, pressure leaderboard, portfolio mix |
| Investor Concentration | mart_housing_pressure | Ownership breakdown by type and ZIP |
| Housing Pressure | mart_housing_pressure | Composite pressure scores, ZIP table |
| SOH Protection | mart_housing_pressure | Save Our Homes differential rankings |
| Housing + Overdose | mart_housing_pressure | Pressure-to-overdose scatter, health trends |
| Development Activity | mart_permit_velocity | Permit volume and stall rate by district |
| Engineering Proof | — | dbt lineage summary, data contract, limitations |

---

## Known Limitations

**CAMA is current-year only.** Multi-year trend analysis requires historical rolls from the Florida DOR data portal (free request, ~3 business days).

**AMANDA permits exclude incorporated cities.** Daytona Beach, Ormond Beach, and other municipalities issue permits through separate systems not included here.

**Owner classification is probabilistic.** Some seasonal residents are misclassified as out-of-state investors, particularly in coastal ZIPs (32118, 32127). Low-confidence classifications are excluded from headline metrics.

**Census ACS is a 5-year average.** The 2024 release covers 2020–2024 and should be treated as structural neighborhood context, not current-year data.

**Pressure score is relative, not absolute.** The composite score is min-max normalized within the visible ZIP set. It reflects relative pressure within the current view and shifts when filters are applied.