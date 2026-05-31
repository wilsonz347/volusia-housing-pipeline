with source as (
    select * from {{ source('raw', 'raw_cama_sales') }}
),

parsed_dates as (
    select
        *,
        TO_TIMESTAMP(NULLIF(TRIM(saledt), ''), 'MM/DD/YY HH24:MI:SS') AS sale_ts_raw
    from source
),

stg_sales as (
    select
        CAST(parid AS TEXT)                                                    AS parcel_id,
        taxyr                                                                  AS assessment_year,
        CAST(sale_ts_raw AS DATE)                                              AS sale_date,
        CAST(price AS NUMERIC)                                                 AS sale_price,
        steb                                                                   AS sale_qualification_code,
        CAST(aprtot AS NUMERIC)                                                AS appraised_total,
        ROUND(CAST(price AS NUMERIC) / NULLIF(CAST(aprtot AS NUMERIC), 0), 3) AS price_to_appraised_ratio,
        ingested_at,
        source
    from parsed_dates
    where parid is not null
),

filtered as (
    select
        *,
        EXTRACT(YEAR FROM sale_date) AS sale_year
    from stg_sales
)

select *
from filtered
where sale_year BETWEEN 1970 AND 2026
and price_to_appraised_ratio BETWEEN 0.1 AND 10