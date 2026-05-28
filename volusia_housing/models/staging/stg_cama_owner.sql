with source as (
    select * from {{ source('raw', 'raw_cama_owner') }}
),

stg_owner as (
    SELECT 
        CAST(parid AS TEXT) AS parcel_id,
        taxyr AS assessment_year,
        ownseq AS owner_sequence,
        UPPER(TRIM(own1)) AS primary_owner_name,
        UPPER(TRIM(COALESCE(own2, ''))) AS secondary_owner_name,
        TRIM(addr1) AS mailing_address_line1,
        TRIM(COALESCE(addr2, '')) AS mailing_address_line2,
        -- State
        CASE 
            WHEN LENGTH(TRIM(SPLIT_PART(TRIM(addr3), ' ', -1))) = 2
            AND TRIM(SPLIT_PART(TRIM(addr3), ' ', -1)) ~ '^[A-Z]+$'
            THEN TRIM(SPLIT_PART(TRIM(addr3), ' ', -1))
            ELSE NULL
        END AS mailing_state,
        -- City
        TRIM(REGEXP_REPLACE(TRIM(addr3), '\s+\S+$', '')) AS mailing_city,
        CASE 
            WHEN country IS NOT NULL AND country != ''
            THEN TRUE
            ELSE FALSE
        END AS is_foreign_owner,
        COALESCE(
            own1 ~* '\m(LLC|L\.L\.C|INC|CORP|CORPORATION|LP|L\.P|LLP|L\.L\.P|LTD|LIMITED|REIT|PLLC|PA|P\.A|NA|N\.A)\M'
            OR own1 ~* '\m(HOLDINGS?|PROPERT(Y|IES)|REALTY|INVESTMENTS?|VENTURES?|ENTERPRISES?|CAPITAL|PARTNERS?|ASSOCIATES|MANAGEMENT|MGMT|GROUP)\M'
            OR own1 ILIKE '%REAL ESTATE%'
            OR own2 ~* '\m(LLC|INC|CORP|LP|LLP|TRUST)\M',
            false
        ) AS is_llc,

        COALESCE(
            own1 ~* '\m(TRUST|TRUSTEE|REVOCABLE|IRREVOCABLE)\M'
            OR own1 ILIKE '%LIVING TRUST%'
            OR own1 ILIKE '%FAMILY TRUST%'
            OR own2 ~* '\m(TRUST|TRUSTEE)\M',
            false
        ) AS is_trust,
        owntype1 AS ownership_type_code,
        owntype1_desc AS ownership_type_description,
        ingested_at,
        source
    FROM source
    WHERE parid IS NOT NULL
)

SELECT * FROM stg_owner