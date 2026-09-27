-- Sanity check across sources: measures CMS flags as "better" should, on average,
-- sit on the good side of the national median. A failure here signals a direction bug.
select domain, avg(diff_from_national_median) as avg_diff
from {{ ref('fct_measure_scores') }}
where comparison_category = 'better' and domain <> 'Uncatalogued'
group by 1
having avg(diff_from_national_median) > 0
