-- Current view of every hospital in the latest CMS release (SCD Type 1).
with latest as (
    select * from {{ ref('stg_cms__hospitals') }}
    where release_date = (select max(release_date) from {{ ref('stg_cms__hospitals') }})
)

select
    l.facility_id::varchar               as facility_id,
    l.facility_name::varchar             as facility_name,
    l.address::varchar                   as address,
    l.city::varchar                      as city,
    l.state::varchar                     as state,
    r.state_name::varchar                as state_name,
    r.census_region::varchar             as census_region,
    l.zip_code::varchar                  as zip_code,
    l.county::varchar                    as county,
    l.hospital_type::varchar             as hospital_type,
    l.ownership::varchar                 as ownership,
    l.ownership_group::varchar           as ownership_group,
    l.has_emergency_services::boolean    as has_emergency_services,
    l.is_birthing_friendly::boolean      as is_birthing_friendly,
    l.overall_rating::integer            as overall_rating,
    l.release_date::date                 as release_date
from latest l
left join {{ ref('state_regions') }} r using (state)
