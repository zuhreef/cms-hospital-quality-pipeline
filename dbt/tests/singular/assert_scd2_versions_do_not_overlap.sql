-- Each hospital's SCD2 versions must tile time without overlap and have exactly one open version at most.
with v as (
    select
        facility_id, valid_from, valid_to,
        lead(valid_from) over (partition by facility_id order by valid_from) as next_from
    from {{ ref('dim_hospital_history') }}
)
select * from v
where (next_from is not null and valid_to is distinct from next_from)
   or (valid_to is not null and valid_to <= valid_from)
