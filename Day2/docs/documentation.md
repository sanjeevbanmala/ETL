## 1. Let me expalain about different sql files in schema.
* `Schema/create_raw_employee_table.sql` file:
``` 
CREATE TABLE raw_employee(
	employee_id VARCHAR(500),
    first_name VARCHAR(500),
    last_name VARCHAR(500),
    department_id VARCHAR(500),
    department_name VARCHAR(500),
    manager_employee_id VARCHAR(500),
    employee_role VARCHAR(500),
    salary VARCHAR(500),
    hire_date VARCHAR(500),
    terminated_date VARCHAR(500),
    terminated_reason VARCHAR(500),
    dob VARCHAR(500),
    fte VARCHAR(500),
    location VARCHAR(500)
);
```
This table `raw_employee` is created to store data available in json format which is `employee_2021_08_01.json`

All columns are declared `VARCHAR(500)` because data can not be in the same format as the database requires which may cause problem in bulk import.
For example: Format for date `2021-12-12` can come as `12-12-2021` and thus declaring datatypes may cause problem later. So, lets create table with columns all VARCHAR types.

* `schema/create_raw_employee_archive.sql` file:
```
CREATE TABLE raw_employee_archive(
    employee_id VARCHAR(500), 
    first_name VARCHAR(500),
    last_name VARCHAR(500), 
    department_id VARCHAR(500), 
    department_name VARCHAR(500), 
    manager_employee_id VARCHAR(500), 
    employee_role VARCHAR(500), 
    salary VARCHAR(500), 
    hire_date VARCHAR(500), 
    terminated_date VARCHAR(500), 
    terminated_reason VARCHAR(500), 
    dob VARCHAR(500), 
    fte VARCHAR(500), 
    location VARCHAR(500),
	sheet_name VARCHAR(500)
);

```
This table `raw_employee_archive` stores as a backup of data extracted in `raw_employee` as the `raw_employee` table gets deleted each time data is loaded into it.

* `schema/create_raw_xml_employee.sql` file:
```
CREATE TABLE raw_employee_xml(
    employee_id VARCHAR(500), 
    first_name VARCHAR(500),
    last_name VARCHAR(500), 
    department_id VARCHAR(500), 
    department_name VARCHAR(500), 
    manager_employee_id VARCHAR(500), 
    employee_role VARCHAR(500), 
    salary VARCHAR(500), 
    hire_date VARCHAR(500), 
    terminated_date VARCHAR(500), 
    terminated_reason VARCHAR(500), 
    dob VARCHAR(500), 
    fte VARCHAR(500), 
    location VARCHAR(500)
);

```
This table `raw_employee_xml` is created to store data available in xml format which is `employee_2021_08_01.xml`

All columns are declared `VARCHAR(500)` because data can not be in the same format as the database requires which may cause problem in bulk import.
For example: Format for date `2021-12-12` can come as `12-12-2021` and thus declaring datatypes may cause problem later. So, lets create table with columns all VARCHAR types.

* `schema/create_raw_employee_xml_archive.sql` file:
```
CREATE TABLE raw_employee_xml_archive(
    employee_id VARCHAR(500), 
    first_name VARCHAR(500),
    last_name VARCHAR(500), 
    department_id VARCHAR(500), 
    department_name VARCHAR(500), 
    manager_employee_id VARCHAR(500), 
    employee_role VARCHAR(500), 
    salary VARCHAR(500), 
    hire_date VARCHAR(500), 
    terminated_date VARCHAR(500), 
    terminated_reason VARCHAR(500), 
    dob VARCHAR(500), 
    fte VARCHAR(500), 
    location VARCHAR(500),
	sheet_name VARCHAR(500)
);

```

This table `raw_employee_xml_archive` stores as a backup of data extracted in `raw_employee_xml` as the `raw_employee_xml` table gets deleted each time data is loaded into it.

* `schema/create_raw_timesheet.sql` file:
```
CREATE TABLE raw_timesheet(
	employee_id VARCHAR(500),
	cost_center VARCHAR(500),
	punch_in_time VARCHAR(500),
	punch_out_time VARCHAR(500),
	punch_apply_date VARCHAR(500),
	hours_worked VARCHAR(500),
	paycode VARCHAR(500)
);

```
This table `raw_timesheet` is created to store data available in csv format which is `timesheet_2021_05_23.csv` `timesheet_2021_06_23.csv` `timesheet_2021_07_24.csv`

* `schema/create_raw_timesheet_archive.sql` file:
```
CREATE TABLE raw_timesheet_archive(
	employee_id VARCHAR(500),
	cost_center VARCHAR(500),
	punch_in_time VARCHAR(500),
	punch_out_time VARCHAR(500),
	punch_apply_date VARCHAR(500),
	hours_worked VARCHAR(500),
	paycode VARCHAR(500),
	sheet_name VARCHAR(500)
);

```
This table `raw_timesheet_archive` stores as a backup of data extracted in `raw_timesheet` as the `raw_timesheet` table gets deleted each time data is loaded into it.

## 2. `connection.py` file in  src/pipeline/connection.py.
A connection object is used each time to connect with Postgres Database. The connection can be made through `psycopg2` library in python. So, i had created a separate file for connection with database which can be used in other .py files by using command `from connection import connect`
```
import psycopg2

def connect():
    return psycopg2.connect( 
        host = "localhost", 
        database = "extraction_database", 
        user ="postgres", 
        password ="password", 
        port =5432)

```

Here, host is the server, database is the databse to be used, user and password is the credentials to acces the mentioned databse and port is the port of databse.

I have not included the closing of `connect()` here and i have closed the connection object in respective python scripts.