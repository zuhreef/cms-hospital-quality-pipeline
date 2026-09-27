{#-
  Slowly Changing Dimension Type 2 built from release partitions.

  Rather than relying on `dbt snapshot` (which only sees the source at the
  moment it runs), history is derived from every CMS release kept in the
  lake. That makes it fully reproducible and back-fillable: replaying the
  bronze layer rebuilds exactly the same history.

  A new version starts when any tracked attribute changes. A hospital that
  disappears from a later release is closed out (valid_to = first release it
  is missing from).
-#}
with hospitals as (
    select
        *,
        md5(concat_ws('|',
            facility_name, hospital_type, ownership,
            coalesce(overall_rating::varchar, 'NA'),
            has_emergency_services::varchar, is_birthing_friendly::varchar
        )) as attr_hash
    from {{ ref('stg_cms__hospitals') }}
),

releases as (
    select
        release_date,
        lead(release_date) over (order by release_date) as next_release
    from (select distinct release_date from hospitals)
),

marked as (
    select
        *,
        lag(attr_hash) over (partition by facility_id order by release_date) as prev_hash
    from hospitals
),

versions as (
    select
        *,
        lead(release_date) over (partition by facility_id order by release_date) as next_version_start
    from marked
    where prev_hash is distinct from attr_hash
),

last_seen as (
    select facility_id, max(release_date) as last_release
    from hospitals
    group by 1
)

select
    md5(v.facility_id || '|' || v.release_date::varchar)      as hospital_version_key,
    v.facility_id,
    v.facility_name,
    v.state,
    v.hospital_type,
    v.ownership,
    v.ownership_group,
    v.overall_rating,
    v.has_emergency_services,
    v.is_birthing_friendly,
    v.attr_hash,
    v.release_date                                             as valid_from,
    coalesce(v.next_version_start, r.next_release)            as valid_to,
    coalesce(v.next_version_start, r.next_release) is null    as is_current
from versions v
join last_seen ls using (facility_id)
left join releases r on r.release_date = ls.last_release
