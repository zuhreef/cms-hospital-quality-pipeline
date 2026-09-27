select
    s.state,
    s.state_name,
    s.census_region,
    count(*)                                                                   as hospitals,
    count(*) filter (where s.overall_rating is not null)                       as rated_hospitals,
    round(avg(s.overall_rating), 2)                                            as avg_overall_rating,
    round(100.0 * count(*) filter (where s.overall_rating >= 4)
          / nullif(count(*) filter (where s.overall_rating is not null), 0), 1) as pct_4_or_5_star,
    round(avg(s.patient_experience_star), 2)                                   as avg_patient_experience_star,
    round(avg(s.outcome_composite), 1)                                         as avg_outcome_composite,
    sum(s.measures_better)                                                     as measures_better,
    sum(s.measures_worse)                                                      as measures_worse,
    round(100.0 * sum(s.measures_worse) / nullif(sum(s.measures_rated), 0), 2) as pct_measures_worse
from {{ ref('mart_hospital_scorecard') }} s
group by 1, 2, 3
