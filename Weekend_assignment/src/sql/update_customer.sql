UPDATE raw_customer_archive 
SET active='true'
WHERE active='Y';

TRUNCATE TABLE town RESTART IDENTITY;

INSERT INTO town(town_name)
SELECT DISTINCT town FROM raw_customer_archive;

DELETE FROM customers;

INSERT INTO customers
SELECT 
  CAST(c.customer_id AS INT),
  c.user_name,
  CAST(INITCAP(c.first_name) AS VARCHAR) AS first_name,
  CAST(INITCAP(c.last_name) AS VARCHAR) AS last_name,
  CAST(INITCAP(c.country) AS VARCHAR) AS country,
  t.town_id,
  CAST(c.active AS bool)
FROM raw_customer_archive c
INNER JOIN town t on t.town_name=c.town;