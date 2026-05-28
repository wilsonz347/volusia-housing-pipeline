with source as (
    select * from {{ source('raw', 'raw_cama_parcel') }}
),

stg_parcel as (
    SELECT
        CAST(parid AS TEXT) AS parcel_id,
        taxyr AS assessment_year,
        luc AS land_use_code,
        TRIM(luc_desc) AS land_use_description,
        CAST(aprtot AS NUMERIC) AS appraised_total,
        CAST(sasd AS NUMERIC) AS school_assessed_value,
        CAST(nsasd AS NUMERIC) AS nonschool_assessed_value,
        CAST(stxbl AS NUMERIC) AS school_taxable_value,
        CAST(nstxbl AS NUMERIC) AS nonschool_taxable_value,
        (hx_flag = 'Y') AS is_homestead,
        (luc BETWEEN '0100' AND '0900') AS is_residential,
        CASE 
            WHEN hx_flag = 'Y'
            then CAST(aprtot AS NUMERIC) - CAST(sasd AS NUMERIC)
            ELSE null
        END AS soh_differential,
        CAST(COALESCE(livunit, 0) AS NUMERIC) AS living_units,
        nbhd AS neighborhood_code,
        TRIM(nbhd_desc) AS neighborhood_description,
        taxdist AS tax_district_code,
        TRIM(taxdist_desc) AS tax_district_description,
        ingested_at,
        source
    FROM source
    WHERE parid IS NOT NULL
)

SELECT * FROM stg_parcel