with source as (
    select * from {{ source('raw', 'raw_overdose_analysis') }}
),

stg_overdose_analysis as (
    select zip_code, trim(po_name) as po_name, 2021 as overdose_year,
           total2021 as total_overdoses, opiod2021 as opioid_overdoses, ingested_at, source
    from source
    union all
    select zip_code, trim(po_name), 2022,
           total2022, opiod2022, ingested_at, source
    from source
    union all
    select zip_code, trim(po_name), 2023,
           total2023, opiod2023, ingested_at, source
    from source
    union all
    select zip_code, trim(po_name), 2024,
           total2024, opiod2024, ingested_at, source
    from source
)

select * from stg_overdose_analysis