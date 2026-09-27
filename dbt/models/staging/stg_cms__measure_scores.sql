select
    facility_id,
    measure_id,
    measure_name,
    compared_to_national,
    {{ comparison_category('compared_to_national') }}  as comparison_category,
    denominator,
    score,
    lower_estimate,
    higher_estimate,
    number_of_patients,
    number_of_patients_returned,
    footnote,
    period_start,
    period_end,
    source_dataset,
    release_date::date                                  as release_date,
    _ingested_at                                        as ingested_at
from {{ source('silver', 'measure_scores') }}
