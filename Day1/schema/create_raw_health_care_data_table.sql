CREATE TABLE fact_employee(
    employee_id INT,
	department_id INT,
	manager_employee_id INT,
	role_id INT,
	salary INT,
	weekly_hours INT
);

CREATE TABLE fact_timesheet(
	employee_id INT,
	punch_apply_date DATE,
	department_id INT,
	hours_worked FLOAT,
	shift_type_id INT,
	punch_in_time TIMESTAMP,
	punch_out_time TIMESTAMP,
	time_period_id INT,
	attendance BOOL,
	work_code VARCHAR(10),
	has_taken_break BOOL,
	break_hour FLOAT,
	was_charge BOOL,
	charge_hour FLOAT,
	on_call BOOL,
	on_call_hour FLOAT,
	is_weekend BOOL,
	num_teammates_absent INT
);

CREATE TABLE dim_shift_type(
	shift_type_id SERIAL,
	shift_type VARCHAR(7),
	PRIMARY KEY (shift_type_id)
);

CREATE TABLE dim_period(
	period_id SERIAL,
	star_date DATE,
	end_date DATE,
	PRIMARY KEY (period_id)
);

CREATE TABLE dim_department(
	department_id  SERIAL,
	department_name VARCHAR(50),
	PRIMARY KEY (department_id)
);

CREATE TABLE dim_role(
	role_id  SERIAL,
	role VARCHAR(50),
	PRIMARY KEY (role_id)
);

