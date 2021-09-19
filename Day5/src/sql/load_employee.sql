DELETE FROM fact_employee;
DELETE FROM dim_role;
INSERT INTO dim_role(name)
SELECT DISTINCT employee_role FROM employee;

DELETE FROM dim_status;
INSERT INTO dim_status
VALUES
(1,'Active'),
(2,'Inactive');

DELETE FROM dim_manager;
INSERT INTO dim_manager
SELECT * FROM manager;

INSERT INTO fact_employee
SELECT 
   e.client_employee_id,
   e.first_name,
   e.last_name,
   e.department_id,
   e.manager_employee_id,
   CAST(e.salary as FLOAT),
   e.hire_date,
   e.terminated_date,
   e.terminated_reason,
   e.dob,
   e.fte,
   e.weekly_hours,
   r.id,
   CASE WHEN e.terminated_date IS NULL THEN 1 ELSE 2 END as active_status_id
FROM employee e
INNER JOIN dim_role r ON r.name=e.employee_role;