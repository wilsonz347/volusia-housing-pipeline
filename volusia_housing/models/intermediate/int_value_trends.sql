with parcels as (
    select * from {{ ref('stg_cama_parcel') }}
),

situs as (
    -- Deduplicate: some parcels have multiple situs records despite primary filter
    select distinct on (parcel_id) 
        parcel_id,
        zip_code,
        physical_city
    from {{ ref('stg_cama_situs') }}
    order by parcel_id, zip_code nulls last
),

sales_aggregated as (
    select
        parcel_id,
        count(*)                                    as total_sales_count,
        min(sale_date)                              as earliest_sale_date,
        max(sale_date)                              as latest_sale_date,
        round(avg(sale_price))                      as avg_sale_price,
        round(avg(price_to_appraised_ratio), 3)     as avg_price_to_appraised_ratio
    from {{ ref('stg_cama_sales') }}
    group by parcel_id
),

latest_sale as (
    select distinct on (parcel_id)
        parcel_id,
        sale_date                                   as latest_sale_date,
        sale_price                                  as latest_sale_price,
        price_to_appraised_ratio                    as latest_price_to_appraised_ratio,
        sale_year                                   as latest_sale_year
    from {{ ref('stg_cama_sales') }}
    order by parcel_id, sale_date desc nulls last
),

int_value_trends as (
    select
        p.parcel_id,
        p.assessment_year,
        p.land_use_code,
        p.land_use_description,
        p.is_residential,
        p.is_homestead,
        p.appraised_total,
        p.school_assessed_value,
        p.nonschool_assessed_value,
        p.school_taxable_value,
        p.soh_differential,
        p.living_units,
        p.neighborhood_code,
        p.neighborhood_description,
        p.tax_district_code,

        -- SOH benefit bucket for mart aggregation
        case
            when p.is_homestead = false             then 'not_homestead'
            when p.soh_differential = 0             then 'no_benefit_yet'
            when p.soh_differential < 25000         then 'under_25k'
            when p.soh_differential < 75000         then '25k_to_75k'
            when p.soh_differential < 150000        then '75k_to_150k'
            else                                         'over_150k'
        end                                         as soh_benefit_bucket,

        s.zip_code,
        s.physical_city,

        sa.total_sales_count,
        sa.earliest_sale_date,
        sa.latest_sale_date,
        sa.avg_sale_price,
        sa.avg_price_to_appraised_ratio,

        ls.latest_sale_price,
        ls.latest_price_to_appraised_ratio,
        ls.latest_sale_year

    from parcels p
    left join situs s           on p.parcel_id = s.parcel_id
    left join sales_aggregated sa on p.parcel_id = sa.parcel_id
    left join latest_sale ls    on p.parcel_id = ls.parcel_id
)

select * from int_value_trends