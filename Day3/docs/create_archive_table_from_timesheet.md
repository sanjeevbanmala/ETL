# Create archive for data to be stored in json format of day2
The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day2/src/pipeline/extract_timesheet_data.py)

The script for extracting the data has been already documented in Day2/docs.
I have added an if else condition in the existing code mentioned above.

First of all, i have created a sql statement to check if there are records which are to be inserted in the archive already exists in the database.Then, I executed the sql statement.
```
search_sheet = "select employee_id from raw_timesheet_archive where sheet_name = '" + filePath +"'"
cur.execute(search_sheet)

```
The if case:
if the cursor fetchs data from the archive table.
```
if(cur.fetchall()):
        print("archive alreday exists!!!")
```

The else case:
If the data is not in the archive database then the data is stored in the archive table.
```
with open(filePath, 'r') as file:
            i = 0
            for line in file:
                if i == 0:
                    i += 1
                    continue
            row1 = line.split(",")
            row1.append(filePath)
            sql1= """INSERT INTO raw_timesheet_archive(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode,sheet_name)
            VALUES( %s,%s, %s, %s, %s, %s, %s,%s)"""
            cur.execute(sql1, row1)
            con.commit()
            i +=1 
        print("New archive created!!!!")

```
A print message will be given if the data is already available as "Archive already exists!!!"
and as "New archive created!!!!" when the data is not available in archive.