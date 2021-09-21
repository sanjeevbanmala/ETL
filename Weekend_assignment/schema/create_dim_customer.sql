CREATE TABLE dim_customer(
	customer_id VARCHAR(200),
	user_name VARCHAR(200),
	first_name VARCHAR(200),
	last_name VARCHAR(200),
	country VARCHAR(50),
	town_id INT,
	active bool,
	PRIMARY KEY (customer_id),
	FOREIGN KEY (town_id) REFERENCES fact_town(town_id)
);