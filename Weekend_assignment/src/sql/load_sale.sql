DELETE FROM dim_date;

INSERT INTO dim_date
SELECT id, date, year, time, month, day
FROM date;

DELETE FROM dim_employee;

INSERT INTO dim_employee
select * from employee;

INSERT INTO fact_sale
select * from sales;