UPDATE raw_sales_archive
SET bill_date='2017-02-28 11:00:00'
WHERE bill_date='2017-02-30 11:00:00';

TRUNCATE TABLE date RESTART IDENTITY;;
INSERT INTO date(bill_date,date,year,time,month,day)
SELECT DISTINCT bill_date, 
CAST(bill_date as DATE) as date,
EXTRACT(YEAR FROM CAST(bill_date as date)) as year,
CAST(bill_date as TIMESTAMP)::TIMESTAMP::TIME as time,
TO_CHAR(CAST(bill_date as DATE), 'Month') as month,
TO_CHAR(CAST(bill_date as DATE), 'Day') as day
FROM raw_sales_archive;

TRUNCATE TABLE employee RESTART IDENTITY;
INSERT INTO employee(employee_name)
SELECT DISTINCT INITCAP(created_by) FROM  raw_sales_archive;

DROP TABLE IF EXISTS sales;
CREATE TABLE sales AS
SELECT s.id, 
CAST(s.transaction_id AS INT), 
CAST(s.bill_no AS INT), 
d.id AS date_id,
CAST(s.customer_id AS INT),
CAST(s.product_id AS INT), 
CAST(s.qty AS NUMERIC ), 
CAST(s.gross_price AS FLOAT) ,
CAST(s.tax_pc AS FLOAT),
CAST(s.tax_amt AS FLOAT),
CAST(s.discount_pc AS FLOAT),
CAST (s.discount_amt AS FLOAT),
CAST(s.net_bill_amt AS FLOAT),
e.employee_id
FROM raw_sales_archive s
INNER JOIN date d ON d.bill_date=s.bill_date
INNER JOIN employee e ON e.employee_name= INITCAP(s.created_by);