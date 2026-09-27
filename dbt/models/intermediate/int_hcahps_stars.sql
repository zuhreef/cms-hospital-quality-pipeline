-- Pivot the long HCAHPS survey feed to one row per hospital per release.
select
    facility_id,
    release_date,
    max(star_rating)    filter (where measure_id = 'H_STAR_RATING')            as summary_star,
    max(star_rating)    filter (where measure_id = 'H_COMP_1_STAR_RATING')     as nurse_communication_star,
    max(star_rating)    filter (where measure_id = 'H_COMP_2_STAR_RATING')     as doctor_communication_star,
    max(star_rating)    filter (where measure_id = 'H_COMP_3_STAR_RATING')     as staff_responsiveness_star,
    max(star_rating)    filter (where measure_id = 'H_COMP_5_STAR_RATING')     as medicine_communication_star,
    max(star_rating)    filter (where measure_id = 'H_COMP_6_STAR_RATING')     as discharge_info_star,
    max(star_rating)    filter (where measure_id = 'H_COMP_7_STAR_RATING')     as care_transition_star,
    max(star_rating)    filter (where measure_id = 'H_CLEAN_STAR_RATING')      as cleanliness_star,
    max(star_rating)    filter (where measure_id = 'H_QUIET_STAR_RATING')      as quietness_star,
    max(star_rating)    filter (where measure_id = 'H_RECMND_STAR_RATING')     as recommend_star,
    max(answer_percent) filter (where measure_id = 'H_RECMND_DY')              as pct_definitely_recommend,
    max(answer_percent) filter (where measure_id = 'H_HSP_RATING_9_10')        as pct_rating_9_or_10,
    max(completed_surveys)                                                     as completed_surveys,
    max(response_rate_pct)                                                     as response_rate_pct
from {{ ref('stg_cms__hcahps') }}
group by 1, 2
