-- Hospitals whose CMS overall star rating moved between consecutive releases (from SCD2 history).
with h as (
    select
        facility_id, facility_name, state, census_region, hospital_type, ownership_group,
        valid_from,
        overall_rating,
        lag(overall_rating) over (partition by facility_id order by valid_from) as previous_rating
    from {{ ref('dim_hospital_history') }}
)
select
    *,
    overall_rating - previous_rating                                            as rating_change
from h
where previous_rating is not null
  and overall_rating is not null
  and overall_rating <> previous_rating
