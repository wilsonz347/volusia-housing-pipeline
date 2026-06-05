with source as (
    select * from {{ ref('stg_permits') }}
),

aggregated as (
    select
        council_district,
        COUNT(*)                                                           as total_permits,
        -- Residential vs Commercial permits
        COUNT(*) FILTER (WHERE is_residential_permit = true)              as residential_permits,
        COUNT(*) FILTER (WHERE is_residential_permit = false)             as commercial_permits,
        -- Permit status categories
        COUNT(*) FILTER (WHERE permit_status IN ('Issued', 'Notice to Proceed', 'Ready to Proceed')) AS active_permits,
        COUNT(*) FILTER (WHERE permit_status IN ('Plan Review', 'Dept Review', 'Zoning Review', 'In Review', 'Final Prep')) AS in_review_permits,
        COUNT(*) FILTER (WHERE permit_status IN ('Application', 'Building App Intake', 'App Incomplete')) AS intake_permits,
        COUNT(*) FILTER (WHERE permit_status IN ('Hold Issue', 'Ready Issue')) AS held_permits,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY permit_age_days)      as median_permit_age_days,
        COUNT(*) FILTER (WHERE permit_age_days > 180)                     as stalled_permit_count
    from source
    where council_district is not null
    group by council_district
),

mart_permit_velocity as (
    select
        *,
        ROUND(CAST(stalled_permit_count AS NUMERIC)
            / NULLIF(total_permits, 0) * 100, 1)                          as stalled_pct
    from aggregated
)

select * from mart_permit_velocity