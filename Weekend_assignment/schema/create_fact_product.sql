CREATE TABLE fact_product(
	product_id VARCHAR(200),
	product_name VARCHAR(200),
	description VARCHAR(200),
	price DOUBLE PRECISION,
	mrp DOUBLE PRECISION,
	pieces_per_case NUMERIC,
	weight_per_piece NUMERIC,
	uom VARCHAR(10),
	brand VARCHAR(20),
	category_id INT,
	tax_percent DOUBLE PRECISION,
	active BOOL,
	created_by VARCHAR(200),
	created_date TIMESTAMP,
	updated_by VARCHAR(200),
	updated_date TIMESTAMP,
	PRIMARY KEY (product_id),
	FOREIGN KEY (category_id) REFERENCES fact_category(category_id)

);