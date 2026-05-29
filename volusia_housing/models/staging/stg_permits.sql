with source as (
    select * from {{ source('raw', 'raw_permits') }}
),

stg_permits as (
    select
        s.folderrsn AS permit_id,
        s.foldertype AS permit_type_code,
        pt.description AS permit_type_description,
        pt.is_residential AS is_residential_permit,
        trim(s.foldername) AS permit_address,
        trim(s.statusdesc) AS permit_status,
        s.indate AS application_date,
        current_date - DATE(s.indate) AS permit_age_days,
        cast(s.altkey as text) AS parcel_id_ref,
        trim(s.councildistrict) AS council_district,
        s.ingested_at,
        s.source
    from source s
    left join {{ ref('permit_types') }} pt on s.foldertype = pt.foldertype
    where s.folderrsn is not null
)

select * from stg_permits