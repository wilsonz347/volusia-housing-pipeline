-- ZIP level classification
WITH zip_classification AS (
    SELECT
        s.zip_code,

        COUNT(*) AS total_parcels,

        COUNT(*) FILTER (
            WHERE c.is_residential = true
        ) AS residential_parcels,

        COUNT(*) FILTER (
            WHERE c.owner_classification = 'owner_occupied'
              AND c.is_residential = true
        ) AS owner_occupied_count,

        COUNT(*) FILTER (
            WHERE c.owner_classification = 'out_of_state_investor'
              AND c.is_residential = true
        ) AS out_of_state_count,

        COUNT(*) FILTER (
            WHERE c.owner_classification = 'foreign_investor'
              AND c.is_residential = true
        ) AS foreign_count,

        COUNT(*) FILTER (
            WHERE c.owner_classification = 'fl_corporate_investor'
              AND c.is_residential = true
        ) AS fl_corporate_count,

        COUNT(*) FILTER (
            WHERE c.owner_classification = 'trust'
              AND c.is_residential = true
        ) AS trust_count,

        COUNT(*) FILTER (
            WHERE c.owner_classification = 'local_investor'
              AND c.is_residential = true
        ) AS local_investor_count,

        COUNT(*) FILTER (
            WHERE c.is_homestead_final = true
        ) AS homestead_count,

        COUNT(*) FILTER (
            WHERE c.is_homestead_final = true
              AND c.soh_differential > 75000
        ) AS large_soh_gap_count,

        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY c.soh_differential)
            FILTER (
                WHERE c.is_homestead_final = true
            ) AS median_soh_differential,

        AVG(c.soh_differential)
            FILTER (
                WHERE c.is_homestead_final = true
            ) AS avg_soh_differential,

        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY c.appraised_total)
            FILTER (
                WHERE c.is_residential = true
            ) AS median_appraised_value,

        AVG(c.owner_tenure_years)
            FILTER (
                WHERE c.is_residential = true
                  AND c.owner_tenure_years IS NOT NULL
            ) AS avg_owner_tenure_years

    FROM {{ ref('int_owner_classification') }} c
    LEFT JOIN {{ ref('stg_cama_situs') }} s
        ON c.parcel_id = s.parcel_id

    WHERE s.zip_code IS NOT NULL
    GROUP BY s.zip_code
    HAVING COUNT(*) >= 50
),

-- Census data
census AS (
    SELECT * FROM {{ ref('stg_census_acs') }}
),

-- Overdose data
overdose AS (
    SELECT
        zip_code,
        MAX(overdose_rate_per_1000) FILTER (WHERE overdose_year = 2024) AS overdose_rate_2024,
        MAX(overdose_rate_per_1000) FILTER (WHERE overdose_year = 2021) AS overdose_rate_2021
    FROM {{ ref('int_overdose_by_zip') }}
    GROUP BY zip_code
),

mart_housing_pressure AS (
    SELECT
        z.zip_code,
        z.total_parcels,
        z.residential_parcels,
        z.owner_occupied_count,
        z.out_of_state_count,
        z.foreign_count,
        z.fl_corporate_count,
        z.trust_count,
        z.local_investor_count,
        z.homestead_count,
        z.large_soh_gap_count,

        -- High confidence investor metric (out-of-state + foreign)
        ROUND(CAST(z.out_of_state_count + z.foreign_count AS NUMERIC)
            / NULLIF(z.residential_parcels, 0) * 100, 1)                AS high_conf_investor_pct,

        -- Broader investor metric (high + medium confidence)
        ROUND(CAST(z.out_of_state_count + z.foreign_count + z.fl_corporate_count + z.trust_count AS NUMERIC)
            / NULLIF(z.residential_parcels, 0) * 100, 1)                AS all_investor_pct,

        ROUND(CAST(z.owner_occupied_count AS NUMERIC)
            / NULLIF(z.residential_parcels, 0) * 100, 1)                AS owner_occupied_pct,

        z.median_soh_differential,
        z.avg_soh_differential,
        ROUND(
            CAST(z.large_soh_gap_count AS NUMERIC)
            / NULLIF(CAST(z.homestead_count AS NUMERIC), 0)
            * 100,
            1
        ) AS large_soh_gap_pct,
        z.median_appraised_value,
        z.avg_owner_tenure_years,

        -- Census demographics and housing characteristics
        cen.median_household_income,
        cen.median_gross_rent,
        cen.rent_to_income_ratio,
        cen.renter_occupied_ratio,
        cen.population,
        cen.vacant_unit_ratio,

        CASE
            WHEN cen.zip_code IS NOT NULL THEN true
            ELSE false
        END AS has_acs_data,

        -- Overdose rates and change
        o.overdose_rate_2024,
        o.overdose_rate_2021,
        ROUND(CAST(o.overdose_rate_2024 - o.overdose_rate_2021 AS NUMERIC), 2) AS overdose_rate_change

    FROM zip_classification z
    LEFT JOIN census cen ON z.zip_code = cen.zip_code
    LEFT JOIN overdose o ON z.zip_code = o.zip_code
)

SELECT * FROM mart_housing_pressure