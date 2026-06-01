
  
    

  create  table "globant_analytics"."analytics"."mart_departments_above_mean__dbt_tmp"
  
  
    as
  
  (
    

with  __dbt__cte__stg_departments as (
select
    id,
    trim(department) as department
from raw.departments
),  __dbt__cte__stg_hired_employees as (
select
    id,
    trim(name)                              as name,
    datetime::timestamp                     as hired_at,
    extract(year  from datetime::timestamp) as hire_year,
    extract(quarter from datetime::timestamp) as hire_quarter,
    department_id,
    job_id
from raw.hired_employees
), dept_counts as (
    select
        d.id,
        d.department,
        count(e.id)::int as hired
    from __dbt__cte__stg_departments    d
    left join __dbt__cte__stg_hired_employees e
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
  );
  