
  
    

  create  table "globant_analytics"."analytics"."mart_hires_by_quarter__dbt_tmp"
  
  
    as
  
  (
    

with __dbt__cte__stg_hired_employees as (
select
    id,
    trim(name)                              as name,
    datetime::timestamp                     as hired_at,
    extract(year  from datetime::timestamp) as hire_year,
    extract(quarter from datetime::timestamp) as hire_quarter,
    department_id,
    job_id
from raw.hired_employees
),  __dbt__cte__stg_departments as (
select
    id,
    trim(department) as department
from raw.departments
),  __dbt__cte__stg_jobs as (
select
    id,
    trim(job) as job
from raw.jobs
) select
    d.department,
    j.job,
    e.hire_year,
    count(*) filter (where e.hire_quarter = 1)::int as q1,
    count(*) filter (where e.hire_quarter = 2)::int as q2,
    count(*) filter (where e.hire_quarter = 3)::int as q3,
    count(*) filter (where e.hire_quarter = 4)::int as q4
from __dbt__cte__stg_hired_employees e
join __dbt__cte__stg_departments    d on e.department_id = d.id
join __dbt__cte__stg_jobs           j on e.job_id        = j.id
group by d.department, j.job, e.hire_year
order by d.department, j.job, e.hire_year
  );
  