DELETE FROM fact_sale;
DELETE FROM dim_customer;

DELETE FROM dim_town;

INSERT INTO dim_town
SELECT * FROM town;

INSERT INTO dim_customer 
SELECT *  FROM customers;