> # Task to be done:
## Write a script to extract data from a employee_2021_08_01.json file into the database.

The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day2/src/pipeline/extract_timesheet_data.py)

Let me explain how I did this:

## 1. Imported necessary files:
```
from connection import connect
```
## 2. Defining function
```
def extract_timesheet_data(filePath):

```
Here, filePath is the path of csv file. Then, I created a connection object.

```
con = connect()
cur = con.cursor()
```
Then, I deleted existing data in the raw_timesheet table

```
delete_sql = """DELETE FROM raw_timesheet"""
    cur.execute(delete_sql)
    con.commit()
```
Then, I opened the file with
```
with open(filePath, 'r') as file:
```

Now, I used a for loop to insert each line in the file after formatiing it in to be able to store in database.
```
i = 0
        for line in file:
           if i == 0:
                i += 1
                continue
           row = line.split(",")
           sql= """INSERT INTO raw_timesheet(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode)
            VALUES( %s,%s, %s, %s, %s, %s, %s)"""
           cur.execute(sql, row)
           con.commit()
           i +=1 
```
Finally, I closed the cursor and connection
```
ur.close()
con.close()
```

## 5. Function call with correct file-path:
```
if __name__ == "__main__":
    extract_timesheet_data("../../data/timesheet_2021_06_23.csv")
    extract_timesheet_data("../../data/timesheet_2021_05_23.csv")
    extract_timesheet_data("../../data/timesheet_2021_07_24.csv")
```
Finally, data was extracted in raw_timesheet table in databse.