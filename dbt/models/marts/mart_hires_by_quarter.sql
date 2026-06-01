{{ config(materialized='table') }}

select
    d.department,
    j.job,
    e.hire_year,
    count(*) filter (where e.hire_quarter = 1)::int as q1,
    count(*) filter (where e.hire_quarter = 2)::int as q2,
    count(*) filter (where e.hire_quarter = 3)::int as q3,
    count(*) filter (where e.hire_quarter = 4)::int as q4
from {{ ref('stg_hired_employees') }} e
join {{ ref('stg_departments') }}    d on e.department_id = d.id
join {{ ref('stg_jobs') }}           j on e.job_id        = j.id
group by d.department, j.job, e.hire_year
order by d.department, j.job, e.hire_year
