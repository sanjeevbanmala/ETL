CREATE TABLE fact_timesheet(
	id SERIAL PRIMARY KEY,
	employee_id VARCHAR(200),
	department_id VARCHAR(200),
	shift_start_time TIMESTAMP,
	shift_end_time TIMESTAMP,
	shift_date DATE,
	shift_type_id INT,
	is_weekend BOOL,
	time_period_id INT,
	hours_worked NUMERIC,
	attendance VARCHAR(10),
	has_taken_break BOOL,
	break_hour FLOAT,
	was_charge BOOL,
	charge_hour FLOAT,
	was_on_call BOOL,
	on_call_hour FLOAT,
	num_teammates_absent NUMERIC,
	FOREIGN KEY (shift_type_id) REFERENCES dim_shift_type(id),
	FOREIGN KEY (time_period_id) REFERENCES dim_period(id),
	FOREIGN KEY (department_id) REFERENCES dim_department(department_id)
);