-- Every hospital in the current dimension must appear in the scorecard (no fan-out, no drop-out).
select h.facility_id
from {{ ref('dim_hospital') }} h
full outer join {{ ref('mart_hospital_scorecard') }} s using (facility_id)
where h.facility_id is null or s.facility_id is null
