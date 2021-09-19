DELETE FROM fact_date;

INSERT INTO fact_date
SELECT id, date, year, time, month, day
FROM date;

DELETE FROM fact_employee;

INSERT INTO fact_employee
select * from employee;

INSERT INTO fact_sale
select * from sales;