select
    facility_id,
    measure_id,
    question,
    answer_description,
    star_rating,
    answer_percent,
    linear_mean_value,
    completed_surveys,
    response_rate_pct,
    measure_id like '%STAR_RATING'        as is_star_measure,
    period_start,
    period_end,
    release_date::date                    as release_date
from {{ source('silver', 'hcahps') }}
