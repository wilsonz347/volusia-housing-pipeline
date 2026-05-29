with source as (
    select * from {{ source('raw', 'raw_cama_situs') }}
),

stg_situs as (
    SELECT 
        CAST(parid AS TEXT) AS parcel_id,
        taxyr AS assessment_year,
        UPPER(TRIM(cityname)) AS physical_city,
        LEFT(REGEXP_REPLACE(TRIM(zip1), '[^0-9]', '', 'g'), 5) AS zip_code,
        ingested_at,
        source
    FROM source
    WHERE parid IS NOT NULL
)

SELECT * FROM stg_situs