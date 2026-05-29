with source as (
    select * from {{ source('raw', 'raw_census_acs') }}
),

stg_census_acs as (
    select
        TRIM(zcta_name) AS zcta_name,
        zip_code AS zip_code,
        total_population AS population,
        CAST(median_household_income AS NUMERIC) AS median_household_income,
        CAST(median_gross_rent AS NUMERIC) AS median_gross_rent,
        median_home_value AS median_home_value,
        owner_occupied_units AS owner_occupied_units,
        renter_occupied_units AS renter_occupied_units,
        occupied_units AS occupied_units,
        vacant_units AS vacant_units,
        acs_year AS acs_year,
        ROUND(CAST(renter_occupied_units AS NUMERIC) / NULLIF(CAST(occupied_units AS NUMERIC), 0), 3) AS renter_occupied_ratio,
        ROUND(CAST(vacant_units AS NUMERIC) / NULLIF(CAST(occupied_units + vacant_units AS NUMERIC), 0), 3) AS vacant_unit_ratio,
        ROUND(CAST(median_gross_rent * 12 AS NUMERIC) / NULLIF(CAST(median_household_income AS NUMERIC), 0), 3) AS rent_to_income_ratio,
        ingested_at,
        source
    from source
)

select * from stg_census_acs