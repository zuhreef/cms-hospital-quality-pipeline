{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['facility_id', 'measure_id', 'release_date'],
    on_schema_change = 'append_new_columns'
  )
}}
-- Grain: hospital x measure x CMS release. Incremental: only releases newer
-- than what is already loaded are processed on each run.
select
    facility_id,
    measure_id,
    release_date,
    domain,
    source_dataset,
    comparison_category,
    compared_to_national,
    denominator,
    score,
    lower_estimate,
    higher_estimate,
    national_median,
    diff_from_national_median,
    performance_percentile,
    period_start,
    period_end,
    current_timestamp                    as dbt_loaded_at
from {{ ref('int_measure_scores_benchmarked') }}
{% if is_incremental() %}
where release_date > (select coalesce(max(release_date), DATE '1900-01-01') from {{ this }})
{% endif %}
