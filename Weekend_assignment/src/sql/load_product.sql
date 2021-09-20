DELETE FROM dim_product;
DELETE FROM dim_category;

INSERT INTO dim_category
SELECT * FROM category;

INSERT INTO dim_product
SELECT * FROM products;