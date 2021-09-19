# Implement incremental extraction for the HR data feed.
 ## Initial file will be data for all current and historical employees.
## Each new HR file received will only contain the changes i.e. when
* A new employee hired, 
* Employee is terminated
* When any information about the employee changes.
First of all, I created a dummy table for storing the incremental data of 2021-08-02 and 2021-08-03.
As i had already an archive of 2021-08-01 in raw_employee data.
I used following command to create dummy tables where 1=2 copies table schema only and doesnt copy table data.

```
create table raw_employee_incremental1 as select * from raw_employee where 1=2;
create table raw_employee_incremental2 as select * from raw_employee where 1=2;
```
Then, I copied the incremental data to each table.
```
COPY raw_employee_incremental1 FROM 'E:\ETL\Day4\data\employee_incremental_2021_08_02 - Sheet1.csv' WITH CSV header;
COPY raw_employee_incremental2 FROM 'E:\ETL\Day4\data\employee_incremental_2021_08_03 - Sheet1.csv' WITH CSV header;
```
Then, I used a merge statement which checks if the employee id exists in archive table.
* If exists update the terminated date and terminated reason
* If not insert the new record into archive
```
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
```

But unfortunately `Postgresql` does not support `merge` command.
`Merge` is related to the upsert command in `PostgreSQL`, upsert command is introduced from PostgreSQL version 9.5, in `PostgreSQL` there are no command available like `merge`. It is  implementing merging by using `insert on conflict`.
