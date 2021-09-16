# Extract data using a database as source.
The script for this task can be found [here.](https://github.com/sanjeevbanmala/ETL/blob/master/Day3/src/pipeline/extract_sales_data.py)
First of all, i imported necessary libraries which is `psycopg2`
```
import psycopg2
```
Then, I defined a connecton function to connect to source database from where data is to be extracted.

```
def source_connect():
    return psycopg2.connect(
        host="localhost",
        user="postgres", 
        database="ecommerce_source" ,
        password="password", 
        port = 5432
        )
```
Again, I defined a connection function to connect to destination database where data is to be stored or archived.

```
def dest_connect():
    return psycopg2.connect(
        host="localhost",
        user="postgres", 
        database="ecommerce_destination" ,
        password="password", 
        port = 5432
        )

```

The, I defined a function `extract_sales_from_source`.
```
def extract_sales_from_source():
```

I added connection object for both source and destination database connection.
```
sconn=source_connect()
    dconn=dest_connect()

    scur=sconn.cursor()
    dcur=dconn.cursor()

```

Now, I have used a sql query to fetch all necessary sales data by joining multiple tables from source.
```
select_query="""
    select s.user_id, u.username, s.product_id, p.name as product_name, c.id as category_id, c.name as category_name,
    p.price as current_price, s.price as sold_price,s.quantity as sold_quantity,(p.quantity-s.quantity)as remaining_quantity,s.updated_at as sales_date
    from sales s
    inner join users u on s.user_id=u.id
    inner join products p on s.product_id = p.id
    inner join categories c on p.category_id = c.id;"""
```
Then, I executed the above sql statement and fetched it.

```
scur.execute(select_query)
    result=scur.fetchall()
```
Now, I created a sql statement to insert data in destination database.

```
sql='''
    INSERT INTO raw_sales(user_id, username, product_id, product_name, category_id, category_name, current_price, sold_price, sold_quantity, remaining_quantity, sales_date)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
```
Then, I used a for loop to loop each line in the result and execute the statement through cusrsor.
```
for row in result:
        dcur.execute(sql,row)
        dconn.commit()
    
```

Now, The job is done in the function. I closed the cursor and connection object.
```
sconn.close()
dconn.close()
```

Finally, calling the function

```
if __name__ == "__main__":
    extract_sales_from_source()
``

Hence, the data is now extracted from source database to destination database.