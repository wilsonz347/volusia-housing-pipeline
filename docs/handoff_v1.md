# Volusia Housing Pipeline — Project Handoff

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

## In Progress

- Dashboard development
- Dashboard platform implementation (currently Looker Studio)

## Remaining

1. Build dashboard
2. Publish dashboard
3. Submit Florida DOR historical tax roll request
4. Expand monitoring and testing as needed

---

# Technology Stack

- Python 3.14
- PostgreSQL (Supabase)
- dbt Core 1.12
- GitHub Actions
- Looker Studio

## Architecture

```text
Government Data Sources
        ↓
PostgreSQL (Supabase)
        ↓
dbt Models
(staging → intermediate → marts)
        ↓
Dashboard
```

---

# Data Sources

## 1. Volusia County Property Tax Roll (CAMA)

Primary analytical dataset.

The annual CAMA export is ingested into five normalized tables:

- `raw_cama_parcel`
- `raw_cama_owner`
- `raw_cama_sales`
- `raw_cama_situs`
- `raw_cama_exemptions`

These tables provide the foundation for:

- Investor ownership classification
- Homestead analysis
- Save Our Homes (SOH) metrics
- Property valuation metrics
- Housing pressure indicators

### Important

- Only the 2026 tax roll is currently available
- Historical tax rolls are not available
- Multi-year ownership trend analysis is not yet possible

---

## 2. Volusia County Permits (AMANDA)

County permit activity dataset.

### Important

- Covers only unincorporated Volusia County
- Excludes incorporated municipalities (e.g., Daytona Beach)

---

## 3. Census ACS

ZIP-level demographic and housing indicators.

Examples:

- Median household income
- Median rent
- Rent burden
- Vacancy rates
- Occupancy characteristics

### Important

- ACS 2024 5-year estimates
- Represents multi-year averages, not current-year measurements

---

## 4. Volusia Overdose Analysis

ZIP-level overdose statistics.

Coverage:

- 2021–2024

---

# Critical Domain Concepts

## Save Our Homes (SOH) Differential

Formula:

```sql
just_value - assessed_value
```

Interpretation:

- Higher values indicate larger accumulated tax protection
- Strong proxy for long-term ownership
- May indicate displacement risk if ownership changes

---

## Homestead Exemption

Most reliable owner-occupancy indicator available.

Rules:

- Homestead = owner occupied
- Treated as effectively zero false positives

The ownership classification model is built around homestead status.

---

# Investor Ownership Classification

Each residential parcel receives a single ownership classification.

Priority order:

| Priority | Classification | Confidence |
|-----------|---------------|------------|
| 1 | owner_occupied | High |
| 2 | foreign_investor | High |
| 3 | out_of_state_investor | High |
| 4 | fl_corporate_investor | Medium |
| 5 | trust | Medium |
| 6 | local_investor | Low |
| 7 | unclassified | Low |

### Rules

- First matching rule wins
- Homestead status overrides investor indicators
- Local investor category includes some seasonal residents and is lower confidence

---

# Production Analytics Tables

All dashboard reporting should use these mart tables directly.

Business logic has already been implemented in dbt.

Dashboard tools should avoid recreating calculations whenever possible.

---

## mart_housing_pressure

### Grain

One row per ZIP code.

### Coverage

- 29 ZIP codes
- ZIPs with fewer than 50 parcels excluded

### Purpose

Measure investor ownership concentration and housing pressure by ZIP code.

### Primary Inputs

- CAMA tax roll tables
- ACS demographic indicators
- Overdose statistics

### Key Metrics

- `high_conf_investor_pct`
- `all_investor_pct`
- `owner_occupied_pct`
- `median_soh_differential`
- `avg_soh_differential`
- `large_soh_gap_pct`
- `median_appraised_value`
- `avg_owner_tenure_years`
- `median_household_income`
- `median_gross_rent`
- `rent_to_income_ratio`
- `renter_occupied_ratio`
- `vacant_unit_ratio`
- `overdose_rate_2024`
- `overdose_rate_change`

---

## mart_permit_velocity

### Grain

One row per council district.

### Coverage

- 5 Volusia County council districts

### Purpose

Measure permit pipeline activity and development velocity.

### Key Metrics

- `total_permits`
- `residential_permits`
- `commercial_permits`
- `active_permits`
- `in_review_permits`
- `intake_permits`
- `held_permits`
- `median_permit_age_days`
- `stalled_permit_count`
- `stalled_pct`

---

# Key Findings

Validated findings from the current dataset:

- 9.0% of residential parcels are confirmed out-of-state investors
- 16.9% of residential parcels are investor-owned when Florida corporate investors are included
- 65.8% of residential parcels are owner occupied
- ZIP 32114 recorded the highest overdose rate observed in the dataset (13.33 per 1,000 in 2021)
- ZIP 32169 has the highest average SOH differential (~$294k)
- Coastal ZIPs generally exhibit higher investor ownership concentrations and larger SOH benefits than inland ZIPs
- Overdose rates declined substantially across most ZIP codes between 2021 and 2024
- Investor ownership and housing pressure vary significantly across the county

---

# Known Limitations

## Historical Tax Roll Data

- Only 2026 tax roll data is currently available
- No multi-year ownership trend analysis
- No multi-year housing pressure trend analysis

Future enhancement:
- Obtain 2020–2025 historical tax rolls from the Florida Department of Revenue

---

## Sales History

- Sales records are largely complete only through 2008
- Caused by historical CAMA system migration limitations
- Not a filtering issue

---

## Permit Coverage

- Limited to unincorporated Volusia County
- Development activity within municipalities is not represented

---

## Investor Classification

- Local investor category includes some seasonal residents
- Investor ownership may be overstated in certain coastal ZIP codes

---

## ACS Data

- Uses ACS 5-year estimates
- Not current-year measurements

---

## Missing ZIP Codes

- Approximately 12,364 parcels lack usable ZIP code information
- Excluded from ZIP-level aggregation metrics

---

# Dashboard Requirements

The dashboard should answer:

1. Where is investor ownership concentrated?
2. Which ZIP codes have the highest housing pressure?
3. Where do residents have the largest SOH protection?
4. How do housing metrics relate to overdose rates?
5. Where is development activity occurring?

### Recommended Visuals

- KPI cards
- ZIP ranking charts
- Choropleth maps
- Scatter plots
- Correlation analysis
- Permit activity summaries
- Interactive filters

---

# Recommended Next Steps

1. Build housing dashboard from `mart_housing_pressure`
2. Build permit dashboard from `mart_permit_velocity`
3. Design public-facing layout and narrative
4. Publish dashboard
5. Submit Florida DOR historical tax roll request
6. Continue improving monitoring and data quality

---

# Assumptions For Future Conversations

Assume the following are complete and operational:

- Data ingestion pipeline
- PostgreSQL warehouse
- dbt project
- Data quality tests
- Analytics marts
- GitHub Actions automation

Future work should focus primarily on:

- Dashboard design
- Dashboard implementation
- Data storytelling
- Stakeholder delivery
- Historical data acquisition