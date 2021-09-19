DELETE FROM fact_sale;
DELETE FROM fact_customer;

DELETE FROM fact_town;

INSERT INTO fact_town
SELECT * FROM town;

INSERT INTO fact_customer 
SELECT *  FROM customers;