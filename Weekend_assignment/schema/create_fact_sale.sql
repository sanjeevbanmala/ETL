CREATE TABLE fact_sale(
	id VARCHAR(200),
	transaction_id INT,
	bill_no INT,
	date_id INT,
	customer_id VARCHAR(200),
	product_id VARCHAR(200),
	qty NUMERIC,
	gross_price DOUBLE PRECISION,
	tax_pc DOUBLE PRECISION,
	tax_amt DOUBLE PRECISION,
	discount_pc DOUBLE PRECISION,
	discount_amount DOUBLE PRECISION,
	net_bill_amt DOUBLE PRECISION,
	employee_id INT,
	PRIMARY KEY(id),
	FOREIGN KEY (date_id) REFERENCES fact_date(id),
	FOREIGN KEY(customer_id) REFERENCES fact_customer(customer_id),
	FOREIGN KEY(product_id) REFERENCES fact_product(product_id),
	FOREIGN KEY(employee_id) REFERENCES fact_employee(employee_id)
);