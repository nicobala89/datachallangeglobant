select
    id,
    trim(name)                              as name,
    datetime::timestamp                     as hired_at,
    extract(year  from datetime::timestamp) as hire_year,
    extract(quarter from datetime::timestamp) as hire_quarter,
    department_id,
    job_id
from raw.hired_employees
