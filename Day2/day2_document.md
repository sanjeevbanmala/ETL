# DAY 2 Extraction

Knowledge regarding extracting data from multiple formats of data is important as it is not necessary that the client will have its data in csv file always.
I have separated three folders which are:
* data
This folder contains the data to be extracted.
* schema
This folder contains the script for creating the raw data table.
* src
This folder contains pipeline to extract the data.

Under pipeline there are scripts which extract data using python from the json, csv and xml file. I have separated a separtae file connection.py for creating connection with postgres database and imported in required files. 
The extract_employee_data.py file extracts data from json file, extract_employee_xml extracts data from xml file and extract_timesheet_data extracts data from csv file. Also, I have
tried to import data by pandas library and sqlalchemy into postgresql.

Psycopg2 is the driver which connects the python to postgres.

## Important thing to undertake during extraction
* THe datatype of each column should be in text format so that the data format won't create problem like different datatypes.
* Name the table to which data is to be extracted with prefix is better
Copy command can also be used in postgresql to import data directly using SQL.

# SQL Alchemy
SQLAlchemy is the Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL.

It provides a full suite of well known enterprise-level persistence patterns, designed for efficient and high-performing database access, adapted into a simple and Pythonic domain language.

