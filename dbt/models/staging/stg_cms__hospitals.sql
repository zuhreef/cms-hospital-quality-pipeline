select
    facility_id,
    facility_name,
    address,
    city,
    state,
    zip_code,
    county,
    phone,
    hospital_type,
    ownership,
    case
        when ownership like 'Voluntary non-profit%' then 'Non-profit'
        when ownership = 'Proprietary' then 'For-profit'
        when ownership like 'Government%' or ownership in ('Veterans Health Administration', 'Department of Defense', 'Tribal')
            then 'Government'
        when ownership = 'Physician' then 'Physician-owned'
        else 'Other'
    end                                   as ownership_group,
    has_emergency_services,
    is_birthing_friendly,
    overall_rating::integer               as overall_rating,
    overall_rating_footnote,
    mort_measures_better,   mort_measures_no_different,   mort_measures_worse,
    safety_measures_better, safety_measures_no_different, safety_measures_worse,
    readm_measures_better,  readm_measures_no_different,  readm_measures_worse,
    release_date::date                    as release_date,
    _ingested_at                          as ingested_at
from {{ source('silver', 'hospitals') }}
