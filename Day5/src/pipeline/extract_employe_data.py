import psycopg2

def connect():
    return psycopg2.connect(
        host = "localhost", 
        database = "extraction_database", 
        user ="postgres", 
        password ="password", 
        port =5432
        )

def load_timesheet_data(cur,con):
    with open('../sql/load_timesheet.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def extract_employee_data(cur,con):
    with open('../sql/extract_employee.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def load_employee_data(cur,con):
    with open('../sql/load_employee.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()



def main():
    con =connect()
    cur=con.cursor()
    load_timesheet_data(cur,con)
    extract_employee_data(cur,con)
    load_employee_data(cur,con)
    con.close()
    cur.close()



if __name__ == "__main__":
    main()