# Create archive for data to be stored in json format of day2
The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day2/src/pipeline/extract_employee_data.py)

The script for extracting the data has been already documented in Day2/docs.
I have added an if else condition in the existing code mentioned above.

First of all, i have created a sql statement to check if there are records which are to be inserted in the archive already exists in the database.Then, I executed the sql statement.
```
search_sheet = "select employee_id from raw_employee_archive where sheet_name = '" + filePath +"'"
cur.execute(search_sheet)

```
The if case:
if the cursor fetchs data from the archive table.
```
if(cur.fetchall()):
        print("Archive already created")
```

The else case:
If the data is not in the archive database then the data is stored in the archive table.
```
else:
        for i in data:
            columns= ', '.join(str(x).replace('/', '_') for x in i.keys())
            new = ",sheet_name"
            columns= columns + new
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in i.values())
            new1=",'"+str(filePath)+"'"
            values= values + new1
            sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('raw_employee_archive', columns, values)
            cur.execute(sql)
            con.commit()
        print("New archive created")

```
A print message will be given if the data is already available as "Archive already created"
and as "New archive created" when the data is not available in archive.