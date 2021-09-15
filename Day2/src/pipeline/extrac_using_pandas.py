import pandas as pd
from sqlalchemy import create_engine
df=pd.read_json("../../data/employee_2021_08_01.json")
engine = create_engine('postgresql://postgres:password@localhost:5432/car_dealership')
df.to_sql('raw_employee', engine)

#Similarly we can use read_csv and read_xml to directly read the data and insert into postgresql databse using sqlalchemy