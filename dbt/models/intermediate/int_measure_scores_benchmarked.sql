{#-
  Adds national context to every hospital x measure score within a release:
  the national median, the gap to it, and a direction-aware performance
  percentile (100 = best in the nation for that measure).
-#}
with scores as (
    select
        s.*,
        coalesce(c.lower_is_better, true)                    as lower_is_better,
        c.domain,
        c.measure_short_name
    from {{ ref('stg_cms__measure_scores') }} s
    left join {{ ref('measure_catalog') }} c using (measure_id)
),

rated as (
    select * from scores
    where score is not null
      and comparison_category in ('better', 'no_different', 'worse')
),

ranked as (
    select
        facility_id, measure_id, release_date,
        median(score)       over (partition by measure_id, release_date)  as national_median,
        round(100 * case
            when lower_is_better then 1 - percent_rank() over (partition by measure_id, release_date order by score asc)
            else percent_rank() over (partition by measure_id, release_date order by score asc)
        end, 1)                                                           as performance_percentile
    from rated
)

select
    s.*,
    r.national_median,
    round(s.score - r.national_median, 3)                     as diff_from_national_median,
    r.performance_percentile
from scores s
left join ranked r using (facility_id, measure_id, release_date)
