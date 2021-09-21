DROP TABLE IF EXISTS employee;
CREATE TABLE employee AS
SELECT 
 employee_id AS client_employee_id,
 INITCAP(first_name) AS first_name,
 INITCAP(last_name) AS last_name,
 d.department_id AS department_id,
 (CASE WHEN manager_employee_id ='-' THEN NULL ELSE manager_employee_id END ) AS manager_employee_id,
 salary,
 CAST(hire_date AS DATE),
 CAST(CASE WHEN terminated_date='01-01-1700' THEN NULL ELSE terminated_date END AS date) AS terminated_date,
 terminated_reason,
 CAST(dob AS DATE) As dob,
 CAST(fte AS FLOAT) As fte,
 CAST(fte AS FLOAT) * 40 as weekly_hours,
 (CASE WHEN employee_role LIKE '%MGR%' or employee_role LIKE '%Supv%' THEN 'Manager' ELSE 'Employee' END) AS employee_role
 from raw_employee
 join dim_department d ON raw_employee.department_id=d.department_id;

DROP TABLE IF EXISTS manager;

CREATE TABLE manager AS
SELECT DISTINCT
 m.client_employee_id,
 m.first_name,
 m.last_name
FROM employee m
INNER JOIN employee e
ON e.manager_employee_id= m.client_employee_id;