-- SCD Type 2: every historical version of a hospital's tracked attributes.
select
    v.*,
    r.census_region
from {{ ref('int_hospital_versions') }} v
left join {{ ref('state_regions') }} r using (state)
