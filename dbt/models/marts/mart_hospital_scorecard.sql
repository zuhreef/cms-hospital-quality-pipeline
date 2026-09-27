{#-
  One row per active hospital: the single table behind the dashboard's
  hospital explorer. Combines CMS's overall rating, patient experience and a
  transparent outcome composite built from individual measure percentiles.
-#}
{% set min_measures = 5 %}

with latest_release as (
    select max(release_date) as release_date from {{ ref('fct_measure_scores') }}
),

outcomes as (
    select
        facility_id,
        count(*) filter (where comparison_category in ('better', 'no_different', 'worse'))  as measures_rated,
        count(*) filter (where comparison_category = 'better')                              as measures_better,
        count(*) filter (where comparison_category = 'worse')                               as measures_worse,
        avg(performance_percentile)                                                         as avg_measure_percentile,
        avg(performance_percentile) filter (where domain = 'Mortality')                     as mortality_percentile,
        avg(performance_percentile) filter (where domain = 'Readmission')                   as readmission_percentile,
        avg(performance_percentile) filter (where domain = 'Patient safety')                as safety_percentile
    from {{ ref('fct_measure_scores') }}
    where release_date = (select release_date from latest_release)
    group by 1
),

prev_rating as (
    select facility_id, max_by(overall_rating, release_date) as previous_overall_rating
    from {{ ref('stg_cms__hospitals') }}
    where release_date < (select max(release_date) from {{ ref('stg_cms__hospitals') }})
    group by 1
),

px as (
    select * from {{ ref('fct_patient_experience') }}
    where release_date = (select max(release_date) from {{ ref('fct_patient_experience') }})
),

joined as (
    select
        h.facility_id,
        h.facility_name,
        h.city,
        h.state,
        h.state_name,
        h.census_region,
        h.hospital_type,
        h.ownership,
        h.ownership_group,
        h.has_emergency_services,
        h.overall_rating,
        p.previous_overall_rating,
        h.overall_rating - p.previous_overall_rating                         as rating_change,
        px.summary_star                                                     as patient_experience_star,
        px.pct_definitely_recommend,
        px.completed_surveys,
        coalesce(o.measures_rated, 0)                                       as measures_rated,
        coalesce(o.measures_better, 0)                                      as measures_better,
        coalesce(o.measures_worse, 0)                                       as measures_worse,
        coalesce(o.measures_better, 0) - coalesce(o.measures_worse, 0)      as net_outcome_signal,
        round(o.mortality_percentile, 1)                                    as mortality_percentile,
        round(o.readmission_percentile, 1)                                  as readmission_percentile,
        round(o.safety_percentile, 1)                                       as safety_percentile,
        case when o.measures_rated >= {{ min_measures }}
             then round(o.avg_measure_percentile, 1) end                    as outcome_composite
    from {{ ref('dim_hospital') }} h
    left join outcomes o using (facility_id)
    left join prev_rating p using (facility_id)
    left join px using (facility_id)
)

select
    *,
    case
        when outcome_composite is null then 'Insufficient data'
        when outcome_composite >= 70 then 'High performer'
        when outcome_composite >= 55 then 'Above average'
        when outcome_composite >= 45 then 'Average'
        when outcome_composite >= 30 then 'Below average'
        else 'Low performer'
    end                                                                     as outcome_tier,
    case when outcome_composite is not null then
        rank() over (partition by state, outcome_composite is not null order by outcome_composite desc)
    end                                                                     as state_rank
from joined
