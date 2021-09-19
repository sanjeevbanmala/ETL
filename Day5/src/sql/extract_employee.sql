DROP TABLE IF EXISTS employee;
CREATE TABLE employee as
SELECT 
 employee_id as client_employee_id,
 INITCAP(first_name) as first_name,
 INITCAP(last_name) as last_name,
 d.department_id as department_id,
 (CASE WHEN manager_employee_id ='-' THEN NULL ELSE manager_employee_id END ) as manager_employee_id,
 salary,
 CAST(hire_date as DATE),
 CAST(CASE WHEN terminated_date='01-01-1700' THEN NULL ELSE terminated_date END as date) as terminated_date,
 terminated_reason,
 CAST(dob as DATE) as dob,
 CAST(fte as FLOAT) as fte,
 CAST(fte as FLOAT) * 40 as weekly_hours,
 (CASE WHEN employee_role LIKE '%MGR%' or employee_role LIKE '%Supv%' THEN 'Manager' ELSE 'Employee' END) as employee_role
 from raw_employee
 join dim_department d on raw_employee.department_id=d.department_id;

DROP TABLE IF EXISTS manager;

create table manager as
select distinct
 m.client_employee_id,
 m.first_name,
 m.last_name
from employee m
INNER JOIN employee e
on e.manager_employee_id= m.client_employee_id;