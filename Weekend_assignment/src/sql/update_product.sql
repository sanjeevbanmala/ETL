UPDATE raw_products_archive 
SET active='true'
where active='Y';

UPDATE raw_products_archive 
SET active='false'
where active='N';

TRUNCATE TABLE category RESTART IDENTITY;
INSERT INTO category(category)
SELECT DISTINCT category from raw_products_archive;

DELETE FROM products;
INSERT INTO products
select
CAST(p.product_id AS INT),
p.product_name,
p.description,
CAST(p.price AS FLOAT),
CAST(p.mrp AS FLOAT),
CAST(p.pieces_per_case AS NUMERIC),
CAST(p.weight_per_piece AS NUMERIC),
p.uom,
p.brand,
c.category_id,
CAST(p.tax_percent AS FLOAT),
CAST(p.active AS BOOL),
p.created_by,
CAST(p.created_date AS TIMESTAMP),
p.updated_by,
CAST(p.updated_date AS TIMESTAMP)
FROM raw_products_archive p
INNER JOIN category c on c.category = p.category;