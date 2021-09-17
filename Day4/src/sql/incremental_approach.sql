CREATE TABLE raw_employee_incremental1 
AS SELECT * FROM raw_employee where 1=2;

CREATE TABLE raw_employee_incremental2 
AS SELECT * FROM raw_employee where 1=2;

COPY raw_employee_incremental1 FROM 'E:\ETL\Day4\data\employee_incremental_2021_08_02 - Sheet1.csv' WITH CSV header;
COPY raw_employee_incremental2 FROM 'E:\ETL\Day4\data\employee_incremental_2021_08_03 - Sheet1.csv' WITH CSV header;

MERGE INTO raw_employee_archive ra
USING  SELECT employee_id,terminated_date, terminated_reason
       FROM raw_employee_incremental1 ra1
       ON ra.employee_id=t.employee_id

WHEN MATCHED 
  UPDATE SET terminated_date = ra1.terminated_date
  UPDATE SET terminated_reason =ra1.terminated_reason

WHEN NOT MATCHED
  INSERT (employee_id,first_name,last_name,department_id,department_name,manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,fte,location)
  VALUES (ra1.employee_id,ra1.first_name,ra1.last_name,ra1.department_id,ra1.department_name,ra1.manager_employee_id,ra1.employee_role,ra1.salary,ra1.hire_date,ra1.terminated_date,ra1.terminated_reason,ra1.dob,ra1.fte,ra1.location)
;
