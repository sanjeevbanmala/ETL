from connection import connect
import json

def extract_employee_data(filePath):
    con = connect()
    cur = con.cursor()
    f = open(filePath,)
    data = json.load(f)
    
    for i in data:
        columns= ', '.join(str(x).replace('/', '_') for x in i.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in i.values())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('employee', columns, values)
        cur.execute(sql)
        con.commit()
        
    cur.close()
    con.close()
# Closing file
    f.close()
 
# returns JSON object as
# a dictionary
   
 
# Iterating through the json
# list


    #with open(filePath, 'r') as file:
        #i = 0
        #for line in file:
           # if i == 0:
               # i += 1
                #continue
           # row = line.values().split(",")
           # sql= """INSERT INTO employee(employee_id,first_name,last_name,department_id,department_name,manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,fte,location)
            #VALUES(%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"""
            #cur.execute(sql, row)
            #con.commit()
            #i +=1 
    #cur.close()
    #con.close()


if __name__ == "__main__":
    extract_employee_data("../../data/employee_2021_08_01.json")
