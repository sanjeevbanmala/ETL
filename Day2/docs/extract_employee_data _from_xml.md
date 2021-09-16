> # Task to be done:
## Write a script to extract data from a xml file into the database.

The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day2/src/pipeline/extract_employee_xml.py)

Let me explain how I did this:

## 1. Imported necessary libraries:
```
import xml.etree.ElementTree as et
from connection import connect
```
## 2. Creating connection object
```
conn = connect()
cursor = conn.cursor()
```
## 3. Deleting data in existing database
```
delete_sql = """DELETE FROM raw_employee_xml"""
cursor.execute(delete_sql)
conn.commit()
```

## 4. Creating function extract_employee_xml
Let me explain how i did this.
First of all i created a function
```
def extract_employee_xml(filePath):
```
The filePath is the path of the data. 
Then, I used a xml parser to parse the data in xml.

```
employee_tree = et.parse(filePath)
emp = employee_tree.findall('Employee')
```
Then, I created a for loop to loop each data in emp and match each values.
```
for ep in emp:
        employee_id = ep.find('employee_id').text
        first_name = ep.find('first_name').text
        last_name = ep.find('last_name').text
        department_id = ep.find('department_id').text
        department_name = ep.find('department_name').text
        manager_employee_id = ep.find('manager_employee_id').text
        employee_role = ep.find('employee_role').text
        salary = ep.find('salary').text
        hire_date = ep.find('hire_date').text
        terminated_date = ep.find('terminated_date').text
        terminated_reason = ep.find('terminated_reason').text
        dob = ep.find('dob').text
        fte = ep.find('fte').text
        location = ep.find('location').text
```
Then, I inserted the data using sql query.
```
sql ="""INSERT INTO raw_employee_xml(
          "employee_id","first_name","last_name","department_id","department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location"
              )
         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        cursor.execute(sql,(employee_id, first_name, last_name, department_id, department_name, manager_employee_id, employee_role, salary, hire_date, terminated_date, terminated_reason, dob, fte, location))
```
Then, I commited the query and closed the cursor
```
conn.commit()
conn.close()
```
## 5. Function call with correct file-path:
```
if __name__ == "__main__":
    extract_employee_xml("../../data/employee_2021_08_01.xml")
```
Finally data was extracted to raw_employee_xml table.