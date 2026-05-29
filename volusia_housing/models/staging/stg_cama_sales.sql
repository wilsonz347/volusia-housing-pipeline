with source as (
    select * from {{ source('raw', 'raw_cama_sales') }}
),

stg_sales as (
    SELECT
        CAST(parid AS TEXT) AS parcel_id,
        taxyr AS assessment_year,
        TO_DATE(NULLIF(TRIM(saledt), ''), 'MM/DD/YY HH24:MI:SS') AS sale_date,
        EXTRACT(YEAR FROM TO_DATE(NULLIF(TRIM(saledt), ''), 'MM/DD/YY HH24:MI:SS')) AS sale_year,
        CAST(price AS NUMERIC) AS sale_price,
        steb AS sale_qualification_code,
        CAST(aprtot AS NUMERIC) AS appraised_total,
        ROUND(CAST(price AS NUMERIC) / NULLIF(CAST(aprtot AS NUMERIC), 0), 3) AS price_to_appraised_ratio,
        ingested_at,
        source
    FROM source
    WHERE parid IS NOT NULL
)

SELECT * FROM stg_sales