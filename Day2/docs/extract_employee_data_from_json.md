> # Task to be done:
## Write a script to extract data from a employee_2021_08_01.json file into the database.

The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day2/src/pipeline/extract_employee_data.py)

Let me explain how I did this:

## 1. Imported necessary libraries:
```
from connection import connect
import json
```
Here `connection` is the module made in pipeline directory which I have explained in [documentation.md]() file. This helps in easy database connection.

##  2. Defining Function
I defined the `extract_employee_data(filePath)` function which takes `filePath` as argument. `filePath` is the location of `.json` file which is to be extracted into database.
```
def extract_employee_data(filePath):
```
> ## Let me explain what this function does. 
First of all, it connects to required database which is `extraction_database` in my case. Then we define cursor to execute queries later.
```
con = connect()
    cur = con.cursor()
```
Now, i open the file and load the json data
```
f = open(filePath,)
    data = json.load(f)
```
So, we must delete data from existing table
```
delete_employee="delete from raw_employee"
    cur.execute(delete_employee)
    con.commit()
```
Then, looping each data and formatting it to be able to store in the database.
```
for i in data:
        columns= ', '.join(str(x).replace('/', '_') for x in i.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in i.values())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('raw_employee', columns, values)
        cur.execute(sql)
        con.commit()
```
Finally, after the data was committed to the database, the cursor and connection was closed.
```
cur.close()
    con.close()
# Closing file
    f.close()
```
## 3. Function call with correct file-path:
```
if __name__ == "__main__":
    extract_employee_data("../../data/employee_2021_08_01.json")
```
And finally the json data was extracted to raw_employee table.
