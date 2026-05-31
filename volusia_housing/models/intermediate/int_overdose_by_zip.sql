with overdose as (
    select * from {{ ref('stg_overdose_analysis') }}
),

census as (
    select * from {{ ref('stg_census_acs') }}
),

int_overdose_by_zip as (
    select
        o.zip_code,
        o.po_name,
        o.overdose_year,
        o.total_overdoses,
        o.opioid_overdoses,
        c.population,
        c.median_household_income,
        c.renter_occupied_ratio,
        c.rent_to_income_ratio,
        ROUND(CAST(o.total_overdoses AS NUMERIC) / NULLIF(c.population, 0) * 1000, 2) AS overdose_rate_per_1000,
        ROUND(CAST(o.opioid_overdoses AS NUMERIC) / NULLIF(c.population, 0) * 1000, 2) AS opioid_rate_per_1000
    from overdose o
    left join census c on o.zip_code = c.zip_code
)

select * from int_overdose_by_zip