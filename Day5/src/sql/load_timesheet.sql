DELETE FROM fact_timesheet;
DELETE FROM dim_period;
DELETE FROM dim_shift_type;

INSERT INTO dim_period VALUES
(1,'2021-06-22','2021-07-06'),
(2,'2021-07-06','2021-07-20'),
(3,'2021-07-20','2021-07-31');

INSERT INTO dim_shift_type VALUES
(1,'Morning'),
(2,'Evening');

DROP TABLE IF EXISTS new_timesheet;
CREATE TABLE new_timesheet AS
SELECT 
employee_id,
department_id, shift_start_time, shift_end_time,
CAST(shift_date as DATE),
CASE 
WHEN
shift_type = 'Morning'
THEN 1
WHEN
shift_type = 'Evening'
THEN 2
ELSE 1
END As shift_type_id
,
CASE WHEN TO_CHAR(CAST(shift_date as DATE), 'Day') Like 'S%' 
THEN 'true'
ELSE false  END AS is_weekend,
CASE 
WHEN 
CAST(shift_date as DATE) BETWEEN '2021-06-22' AND '2021-07-06'
THEN 1
WHEN
CAST(shift_date as DATE) BETWEEN '2021-07-06' AND '2021-07-20'
THEN 2
WHEN
CAST(shift_date as DATE) BETWEEN '2021-07-20' AND '2021-07-31'
THEN 3
ELSE 1 
END AS time_period_id,
hours_worked,
attendance,has_taken_break, break_hour, was_charge,charge_hour,was_on_call, on_call_hour,num_teammates_absent
from transformation_timesheet;

DELETE FROM dim_department;
INSERT INTO dim_department
SELECT DISTINCT department_id, department_name FROM raw_employee;

INSERT into fact_timesheet(employee_id,department_id, shift_start_time, shift_end_time,shift_date,shift_type_id,is_weekend,time_period_id,hours_worked,attendance,has_taken_break,
	break_hour,was_charge,charge_hour,was_on_call,on_call_hour,num_teammates_absent) 
SELECT * FROM new_timesheet;