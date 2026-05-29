with source as (
    select * from {{ source('raw', 'raw_cama_exemptions') }}
),

stg_exemptions as (
    SELECT 
        CAST(parid AS TEXT) AS parcel_id,
        taxyr AS assessment_year,
        excode AS exemption_code,
        TRIM(excode_desc) AS exemption_description,
        yrbeg AS exemption_year_begin,
        ingested_at,
        source
    FROM source
    WHERE parid IS NOT NULL
)

SELECT * FROM stg_exemptions