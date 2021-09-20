CREATE TABLE dim_date(
	id SERIAL,
	date date,
	year VARCHAR(4),
	time VARCHAR(8),
	month VARCHAR(15),
	day VARCHAR(15),
	PRIMARY KEY (id)
);