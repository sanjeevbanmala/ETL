# Create archive for data to be stored in json format of day2
The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day2/src/pipeline/extract_employee_xml.py)

The script for extracting the data has been already documented in Day2/docs.
I have added an if else condition in the existing code mentioned above.

First of all, i have created a sql statement to check if there are records which are to be inserted in the archive already exists in the database.Then, I executed the sql statement.
```
search_sheet = "select employee_id from raw_employee_xml_archive where sheet_name = '" + filePath +"'"
cursor.execute(search_sheet)

```
The if case:
if the cursor fetchs data from the archive table.
```
if(cursor.fetchall()):
        print("Archive already done!!!")
```

The else case:
If the data is not in the archive database then the data is stored in the archive table.
```
else:
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
            sheet_name="'"+filePath+"'"
            location = ep.find('location').text
            sql1 ="""INSERT INTO raw_employee_xml_archive(
              "employee_id","first_name","last_name","department_id","department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location","sheet_name"
              )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
            cursor.execute(sql1,(employee_id, first_name, last_name, department_id, department_name, manager_employee_id, employee_role, salary, hire_date, terminated_date, terminated_reason, dob, fte, location,filePath))
        print("New archive has been stored")

```
A print message will be given if the data is already available as "Archive already created"
and as "New archive has been stored" when the data is not available in archive.