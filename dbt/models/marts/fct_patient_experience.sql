-- Grain: hospital x CMS release. HCAHPS survey stars and headline percentages.
select * from {{ ref('int_hcahps_stars') }}
