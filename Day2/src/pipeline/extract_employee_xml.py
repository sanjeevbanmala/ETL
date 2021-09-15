import xml.etree.ElementTree as et
from connection import connect
employee_tree = et.parse('../../data/employee_2021_08_01.xml')
emp = employee_tree.findall('Employee')
conn = connect()
cursor = conn.cursor()
delete_sql = """DELETE FROM employee"""
cursor.execute(delete_sql)
conn.commit()
for ep in emp:
    employee_id = ep.find('employee_id').text
    first_name = ep.find('first_name').text
    last_name = ep.find('last_name').text
    department_id = ep.find('department_id').text
    department_name = ep.find('department_name').text
    manager_employee_id = ep.find('manager_employee_id').text
    employee_role = ep.find('employee_role').text
    salary = ep.find('salary').text
    hire_date = ep.find('hire_date').text
    terminated_date = ep.find('terminated_date').text
    terminated_reason = ep.find('terminated_reason').text
    dob = ep.find('dob').text
    fte = ep.find('fte').text
    location = ep.find('location').text
    sql ="""INSERT INTO employee(
    "employee_id","first_name","last_name","department_id","department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location"
    )
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    cursor.execute(sql,(employee_id, first_name, last_name, department_id, department_name, manager_employee_id, employee_role, salary, hire_date, terminated_date, terminated_reason, dob, fte, location))
conn.commit()
conn.close()