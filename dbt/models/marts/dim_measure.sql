with observed as (
    select
        measure_id,
        max_by(measure_name, release_date)    as cms_measure_name,
        max_by(source_dataset, release_date)  as source_dataset
    from {{ ref('stg_cms__measure_scores') }}
    group by 1
)

select
    o.measure_id,
    coalesce(c.measure_short_name, o.cms_measure_name)  as measure_short_name,
    o.cms_measure_name,
    coalesce(c.domain, 'Uncatalogued')                  as domain,
    coalesce(c.lower_is_better, true)                   as lower_is_better,
    c.unit,
    o.source_dataset
from observed o
left join {{ ref('measure_catalog') }} c using (measure_id)
