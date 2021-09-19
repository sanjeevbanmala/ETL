DELETE FROM fact_product;
DELETE FROM fact_category;

INSERT INTO fact_category
SELECT * FROM category;

INSERT INTO fact_product
SELECT * FROM products;