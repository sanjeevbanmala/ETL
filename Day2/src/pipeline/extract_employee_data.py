from connection import connect
import json

def extract_employee_data(filePath):
    con = connect()
    cur = con.cursor()
    f = open(filePath,)
    data = json.load(f)
    delete_employee="delete from raw_employee"
    cur.execute(delete_employee)
    con.commit()
    for i in data:
        columns= ', '.join(str(x).replace('/', '_') for x in i.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in i.values())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('raw_employee', columns, values)
        cur.execute(sql)
        con.commit()
    search_sheet = "select employee_id from raw_employee_archive where sheet_name = '" + filePath +"'"
    cur.execute(search_sheet)
    if(cur.fetchall()):
        print("Archive already created")
    else:
        for i in data:
            columns= ', '.join(str(x).replace('/', '_') for x in i.keys())
            new = ",sheet_name"
            columns= columns + new
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in i.values())
            new1=",'"+str(filePath)+"'"
            values= values + new1
            sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('raw_employee_archive', columns, values)
            cur.execute(sql)
            con.commit()
        print("New archive created")
        
    cur.close()
    con.close()
# Closing file
    f.close()


if __name__ == "__main__":
    extract_employee_data("../../data/employee_2021_08_01.json")
