# Volusia Housing Pipeline — Project Handoff v2

## Project Overview

The Volusia Housing Pipeline is an end-to-end data engineering and analytics project designed to identify where absentee investor ownership is concentrated in Volusia County, Florida, and how it relates to housing pressure, affordability, community stability, and public health outcomes.

The project produces production-ready analytics tables that support a public-facing dashboard for:

- One Voice for Volusia
- Local journalists
- Housing advocates
- Public stakeholders

### Primary Questions

1. What percentage of residential properties are investor-owned in each ZIP code?
2. Which ZIP codes exhibit the greatest housing pressure?
3. Where do long-term residents have the largest Save Our Homes (SOH) tax benefits?
4. How do housing metrics relate to overdose rates and affordability indicators?
5. Where is development activity occurring?

---

# Current Status

## Complete

- Python ingestion pipeline
- PostgreSQL warehouse hosted on Supabase
- dbt transformation layer
- Weekly GitHub Actions automation
- Data quality tests
- Analytics marts
- End-to-end data pipeline operational
- Static HTML dashboard (repo-hosted, no login required)

## Remaining

1. Submit Florida Department of Revenue historical tax roll request (2020–2025)
2. Expand monitoring and alerting as needed

---

# Technology Stack

- Python 3.11+
- PostgreSQL (Supabase)
- dbt Core
- GitHub Actions
- HTML / CSS / JavaScript (static dashboard)

## Architecture

```
Government Data Sources
        ↓
PostgreSQL (Supabase)
        ↓
dbt Models
(staging → intermediate → marts)
        ↓
Static HTML Dashboard (dashboard/)
```

---

# Data Sources

## 1. Volusia County Property Tax Roll (CAMA)

Primary analytical dataset. The annual CAMA export is ingested into five normalized tables:

- `raw_cama_parcel`
- `raw_cama_owner`
- `raw_cama_sales`
- `raw_cama_situs`
- `raw_cama_exemptions`

These tables provide the foundation for investor ownership classification, homestead analysis, Save Our Homes (SOH) metrics, property valuation metrics, and housing pressure indicators.

### Important

- Only the 2026 tax roll is currently available
- Historical tax rolls are not available — multi-year trend analysis is not yet possible
- Sales records are reliably complete only through 2008 due to a historical CAMA system migration

---

## 2. Volusia County Permits (AMANDA)

County permit activity via ArcGIS MapServer.

### Important

- Covers unincorporated Volusia County only
- Excludes incorporated municipalities (e.g. Daytona Beach, Ormond Beach)

---

## 3. U.S. Census ACS

ZIP-level demographic and housing indicators (2024 5-year estimates, covering 2020–2024).

Key fields: median household income, median gross rent, rent burden, vacancy rates, renter occupancy ratio.

### Important

- Represents multi-year averages, not current-year measurements
- Treat as structural neighborhood context, not a 2026 signal

---

## 4. VSO Overdose Analysis

ZIP-level overdose statistics from the Volusia Sheriff's Office via ArcGIS MapServer.

Coverage: 2021–2024.

---

# Critical Domain Concepts

## Save Our Homes (SOH) Differential

```sql
soh_differential = just_value - assessed_value
```

Florida's Save Our Homes amendment caps annual increases in a homestead property's assessed value at 3% or CPI, whichever is lower. Over time this creates a growing gap between market value and taxable value.

A large SOH differential means a long-term resident pays taxes on significantly less than market value. If they sell or are displaced, this protection is lost permanently — their replacement housing is taxed at full market value. This makes the SOH differential a proxy for displacement risk.

## Homestead Exemption

The most reliable owner-occupancy indicator available. A property owner must declare their primary residence to receive the homestead exemption — it is legally verified and requires annual renewal. Treated as effectively zero false positives.

The entire investor ownership classification model is anchored to this field.

---

# Investor Ownership Classification

Each residential parcel receives a single ownership classification via `int_owner_classification` in dbt.

| Priority | Classification | Confidence |
|---|---|---|
| 1 | owner_occupied | High |
| 2 | foreign_investor | High |
| 3 | out_of_state_investor | High |
| 4 | fl_corporate_investor | Medium |
| 5 | trust | Medium |
| 6 | local_investor | Low |
| 7 | unclassified | Low |

### Rules

- First matching rule wins
- Homestead status overrides all investor indicators
- Mailing state is parsed from the `addr3` free-text field (e.g. `DAYTONA BEACH FL`)
- Local investor category includes some seasonal residents — lower confidence, especially in coastal ZIPs

---

# Production Analytics Tables

All dashboard reporting uses these mart tables directly. Business logic is implemented in dbt — do not recreate calculations in the dashboard layer.

## mart_housing_pressure

**Grain:** one row per ZIP code · **Coverage:** 29 ZIP codes (ZIPs with fewer than 50 parcels excluded)

**Purpose:** Measure investor ownership concentration and housing pressure by ZIP code.

**Primary inputs:** CAMA tax roll, ACS demographic indicators, VSO overdose statistics.

**Key metrics:**

| Field | Description |
|---|---|
| `all_investor_pct` | All investor classifications as % of residential parcels |
| `high_conf_investor_pct` | Foreign + out-of-state only (highest confidence) |
| `owner_occupied_pct` | Homestead-derived owner-occupancy rate |
| `median_soh_differential` | Median SOH gap — displacement risk proxy |
| `avg_soh_differential` | Average SOH gap |
| `large_soh_gap_pct` | Share of parcels above large-gap threshold |
| `median_appraised_value` | Median just value by ZIP |
| `avg_owner_tenure_years` | Average years of ownership |
| `median_household_income` | ACS-derived |
| `median_gross_rent` | ACS-derived |
| `rent_to_income_ratio` | ACS-derived rent burden indicator |
| `renter_occupied_ratio` | ACS-derived |
| `vacant_unit_ratio` | ACS-derived |
| `overdose_rate_2024` | VSO overdose rate per 1,000 residents |
| `overdose_rate_change` | Change from 2021 to 2024 |

## mart_permit_velocity

**Grain:** one row per council district · **Coverage:** 5 Volusia County council districts

**Purpose:** Measure permit pipeline activity and development velocity.

**Key metrics:** `total_permits`, `residential_permits`, `commercial_permits`, `active_permits`, `in_review_permits`, `intake_permits`, `held_permits`, `median_permit_age_days`, `stalled_permit_count`, `stalled_pct`

---

# Key Findings

Validated findings from the 2026 dataset:

- **16.4%** of residential parcels are investor-owned county-wide (parcel-weighted)
- **9.8%** are confirmed out-of-state or foreign investors (high-confidence)
- **65.3%** of residential parcels are owner-occupied via homestead exemption
- ZIP **32118** has the highest investor concentration at 40.6% and the highest composite pressure score (68.5)
- ZIP **32169** has the highest median SOH differential ($201k) — highest displacement risk
- Coastal ZIPs exhibit higher investor ownership and larger SOH differentials than inland ZIPs
- Overdose rates declined across nearly all ZIPs between 2021 and 2024
- Rent burden correlates with overdose rates more strongly than investor ownership (r=0.54 vs r=0.26)
- D3 has the highest permit stall rate at 43.1% — 394 permits not progressing

---

# Dashboard

Two dashboard implementations are live:

## Static HTML Dashboard

Located in `dashboard/`. Self-contained, no login required, committed to the repo.

```
dashboard/
├── index.html
├── styles.css
├── app.js
└── data/
    ├── mart_housing_pressure.csv
    └── mart_permit_velocity.csv
```

To run locally:
```bash
cd dashboard
python3 -m http.server 8000
# Open http://localhost:8000
```

To update data: export both marts from Supabase and replace the CSVs in `dashboard/data/`.

**Dashboard tabs:**

| Tab | Description |
|---|---|
| Executive View | KPIs, pressure leaderboard, portfolio mix |
| Investor Concentration | Ownership breakdown by type and ZIP |
| Housing Pressure | Composite pressure scores, full ZIP table |
| SOH Protection | Save Our Homes differential rankings |
| Housing + Overdose | Pressure-to-overdose scatter, health trends |
| Development Activity | Permit volume and stall rate by district |
| Engineering Proof | dbt lineage summary, data contract, limitations |

> **Note:** The dashboard template was AI-assisted. All data and business logic are sourced from dbt marts.

---

# Known Limitations

| Limitation | Detail |
|---|---|
| CAMA current-year only | Only 2026 tax roll available. Multi-year trend analysis requires Florida DOR historical rolls. |
| Sales history gap | Sales records complete only through 2008 due to CAMA system migration. |
| Permit coverage | Unincorporated Volusia County only. Daytona Beach and other municipalities excluded. |
| Local investor confidence | Category includes seasonal residents. Coastal ZIPs (32118, 32127, 32169) most affected. |
| ACS lag | 2024 5-year release covers 2020–2024. Not current-year. |
| Missing ZIP data | ~12,364 parcels lack usable ZIP code — excluded from all ZIP-level aggregations. |
| Pressure score is relative | Composite score is min-max normalized within the visible ZIP set. Shifts when filters are applied. |

---

# Remaining Next Steps

1. Submit Florida Department of Revenue historical tax roll request (2020–2025) — unlocks acceleration analysis
2. Add dbt snapshots to track parcel-level ownership changes week over week
3. Expand monitoring and alerting on dbt test failures

---

# Assumptions For Future Conversations

The following are complete and operational:

- Python ingestion pipeline
- PostgreSQL warehouse (Supabase)
- dbt project (staging → intermediate → marts)
- Data quality tests
- Analytics marts
- GitHub Actions weekly automation
- Static HTML dashboard

Future work should focus on:

- Historical data acquisition (Florida DOR request)
- dbt snapshots for ownership change tracking
- Stakeholder delivery and feedback