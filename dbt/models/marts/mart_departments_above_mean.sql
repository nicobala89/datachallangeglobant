{{ config(materialized='table') }}

with dept_counts as (
    select
        d.id,
        d.department,
        count(e.id)::int as hired
    from {{ ref('stg_departments') }}    d
    left join {{ ref('stg_hired_employees') }} e
        on d.id = e.department_id
        and e.hire_year = 2021
    group by d.id, d.department
),
mean_val as (
    select avg(hired) as m from dept_counts
)
select
    dc.id,
    dc.department,
    dc.hired
from dept_counts dc
cross join mean_val mv
where dc.hired > mv.m
order by dc.hired desc
