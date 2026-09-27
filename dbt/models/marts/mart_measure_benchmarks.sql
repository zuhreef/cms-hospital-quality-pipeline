-- National distribution of each measure, per release: what "good" looks like.
select
    f.measure_id,
    m.measure_short_name,
    m.domain,
    m.unit,
    f.release_date,
    count(*) filter (where f.score is not null)                                         as hospitals_reported,
    round(quantile_cont(f.score, 0.10), 3)                                               as p10,
    round(quantile_cont(f.score, 0.25), 3)                                               as p25,
    round(median(f.score), 3)                                                            as median,
    round(quantile_cont(f.score, 0.75), 3)                                               as p75,
    round(quantile_cont(f.score, 0.90), 3)                                               as p90,
    round(100.0 * count(*) filter (where f.comparison_category = 'better')
          / nullif(count(*) filter (where f.comparison_category in ('better','no_different','worse')), 0), 2) as pct_better,
    round(100.0 * count(*) filter (where f.comparison_category = 'worse')
          / nullif(count(*) filter (where f.comparison_category in ('better','no_different','worse')), 0), 2) as pct_worse
from {{ ref('fct_measure_scores') }} f
join {{ ref('dim_measure') }} m using (measure_id)
group by all
