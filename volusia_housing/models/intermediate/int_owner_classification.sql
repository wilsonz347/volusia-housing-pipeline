with parcels as (
    select * from {{ ref('stg_cama_parcel') }}
),

owners as (
    select * from {{ ref('stg_cama_owner') }}
),

exemptions_pivoted as (
    select
        parcel_id,
        assessment_year,
        max(case when exemption_code = '01'         then 1 else 0 end) as has_homestead_exemption,
        max(case when exemption_code = '10CAP'      then 1 else 0 end) as has_investor_cap,
        max(case when exemption_code = '65'         then 1 else 0 end) as has_senior_exemption,
        max(case when exemption_code in ('06','07') then 1 else 0 end) as has_veteran_exemption,
        2026 - min(
            case when exemption_code in ('01','AH')
            then exemption_year_begin else null end
        )                                                               as owner_tenure_years
    from {{ ref('stg_cama_exemptions') }}
    group by parcel_id, assessment_year
),

int_owner_classification as (
    select
        p.parcel_id,
        p.assessment_year,
        p.land_use_code,
        p.land_use_description,
        p.appraised_total,
        p.soh_differential,
        p.school_assessed_value,
        p.nonschool_assessed_value,
        p.school_taxable_value,
        p.living_units,
        p.neighborhood_code,
        p.neighborhood_description,
        p.tax_district_code,
        p.tax_district_description,
        p.is_residential,

        -- Homestead: hx_flag is primary (legally definitive, more current)
        -- has_homestead_exemption from exemptions table used for reconciliation only
        p.is_homestead                                              as is_homestead_final,
        coalesce(e.has_homestead_exemption, 0)                     as has_homestead_exemption,

        -- Reconciliation field — for validation and audit, not classification
        -- ~1.8% disagreement is a documented timing lag between the two source systems
        case
            when p.is_homestead = true  and coalesce(e.has_homestead_exemption, 0) = 1 then 'confirmed'
            when p.is_homestead = true  and coalesce(e.has_homestead_exemption, 0) = 0 then 'hxflag_only'
            when p.is_homestead = false and coalesce(e.has_homestead_exemption, 0) = 1 then 'exemption_only'
            else                                                                             'not_homestead'
        end                                                        as homestead_reconciliation,

        -- Exemption flags
        coalesce(e.has_investor_cap,      0)                       as has_investor_cap,
        coalesce(e.has_senior_exemption,  0)                       as has_senior_exemption,
        coalesce(e.has_veteran_exemption, 0)                       as has_veteran_exemption,
        e.owner_tenure_years,

        -- Owner fields
        o.primary_owner_name,
        o.mailing_state,
        o.mailing_city,
        o.is_llc,
        o.is_trust,
        o.is_foreign_owner,
        o.ownership_type_code,
        o.ownership_type_description,

        /*
          Owner classification priority hierarchy — applied in order.
          Earlier rules take precedence over later rules.

          1. owner_occupied         — is_homestead = true. Legally definitive. Zero false positives.
          2. foreign_investor       — country field populated. Explicit data signal.
          3. out_of_state_investor  — mailing state != FL. High reliability after staging cleanup.
          4. fl_corporate_investor  — FL mailing + LLC pattern match. Medium confidence.
          5. trust                  — trust pattern match. Ambiguous — may be estate planning.
          6. local_investor         — FL mailing, individual, no homestead. LOW CONFIDENCE catch-all.
                                      Includes landlords, seasonal residents, second-home owners.
                                      Exclude from headline investor metrics — report separately.
          7. unclassified           — null mailing state or other edge cases. Excluded from all metrics.
        */
        case
            when p.is_homestead = true                                   then 'owner_occupied'
            when o.is_foreign_owner = true                               then 'foreign_investor'
            when o.mailing_state != 'FL' and o.mailing_state is not null then 'out_of_state_investor'
            when o.mailing_state = 'FL'  and o.is_llc = true            then 'fl_corporate_investor'
            when o.is_trust = true                                       then 'trust'
            when o.mailing_state = 'FL'  and o.is_llc = false           then 'local_investor'
            else                                                              'unclassified'
        end                                                        as owner_classification,

        -- Confidence level per classification
        -- local_investor is low confidence — too broad, catch-all segment
        case
            when p.is_homestead = true                                   then 'high'
            when o.is_foreign_owner = true                               then 'high'
            when o.mailing_state != 'FL' and o.mailing_state is not null then 'high'
            when o.mailing_state = 'FL'  and o.is_llc = true            then 'medium'
            when o.is_trust = true                                       then 'medium'
            when o.mailing_state = 'FL'  and o.is_llc = false           then 'low'
            else                                                              'low'
        end                                                        as classification_confidence

    from parcels p
    left join owners o on p.parcel_id = o.parcel_id
    left join exemptions_pivoted e on p.parcel_id = e.parcel_id
    where p.parcel_id is not null
)

select * from int_owner_classification