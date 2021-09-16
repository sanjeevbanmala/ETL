from connection import connect



def extract_timesheet_data(filePath):
    con = connect()
    cur = con.cursor()
    delete_sql = """DELETE FROM raw_timesheet"""
    cur.execute(delete_sql)
    con.commit()
    with open(filePath, 'r') as file:
        i = 0
        for line in file:
           if i == 0:
                i += 1
                continue
           row = line.split(",")
           sql= """INSERT INTO raw_timesheet(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode)
            VALUES( %s,%s, %s, %s, %s, %s, %s)"""
           cur.execute(sql, row)
           con.commit()
           i +=1 
    
    search_sheet = "select employee_id from raw_timesheet_archive where sheet_name = '" + filePath +"'"
    cur.execute(search_sheet)
    if(cur.fetchall()):
        print("archive alreday exists!!!")
    else:
        with open(filePath, 'r') as file:
            i = 0
            for line in file:
                if i == 0:
                    i += 1
                    continue
            row1 = line.split(",")
            row1.append(filePath)
            sql1= """INSERT INTO raw_timesheet_archive(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode,sheet_name)
            VALUES( %s,%s, %s, %s, %s, %s, %s,%s)"""
            cur.execute(sql1, row1)
            con.commit()
            i +=1 
        print("New archive created!!!!")
    
    cur.close()
    con.close()


if __name__ == "__main__":
    extract_timesheet_data("../../data/timesheet_2021_06_23.csv")
    extract_timesheet_data("../../data/timesheet_2021_05_23.csv")
    extract_timesheet_data("../../data/timesheet_2021_07_24.csv")
