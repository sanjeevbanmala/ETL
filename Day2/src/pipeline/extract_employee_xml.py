import xml.etree.ElementTree as et
from connection import connect
conn = connect()
cursor = conn.cursor()
delete_sql = """DELETE FROM raw_employee_xml"""
cursor.execute(delete_sql)
conn.commit()

def extract_employee_xml(filePath):
    employee_tree = et.parse(filePath)
    emp = employee_tree.findall('Employee')
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
        sql ="""INSERT INTO raw_employee_xml(
          "employee_id","first_name","last_name","department_id","department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location"
              )
         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        cursor.execute(sql,(employee_id, first_name, last_name, department_id, department_name, manager_employee_id, employee_role, salary, hire_date, terminated_date, terminated_reason, dob, fte, location))
    
    search_sheet = "select employee_id from raw_employee_xml_archive where sheet_name = '" + filePath +"'"
    cursor.execute(search_sheet)
    if(cursor.fetchall()):
        print("Archive already done!!!")
    else:
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
            sheet_name="'"+filePath+"'"
            location = ep.find('location').text
            sql1 ="""INSERT INTO raw_employee_xml_archive(
              "employee_id","first_name","last_name","department_id","department_name","manager_employee_id","employee_role","salary","hire_date","terminated_date","terminated_reason","dob","fte","location","sheet_name"
              )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
            cursor.execute(sql1,(employee_id, first_name, last_name, department_id, department_name, manager_employee_id, employee_role, salary, hire_date, terminated_date, terminated_reason, dob, fte, location,filePath))
        print("New archive has been stored")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    extract_employee_xml("../../data/employee_2021_08_01.xml")