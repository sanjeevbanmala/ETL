from connection import connect

def extract_timesheet_data(filePath):
    con = connect()
    cur = con.cursor()

    with open(filePath, 'r') as file:
        i = 0
        for line in file:
           if i == 0:
                i += 1
                continue
           row = line.split(",")
           sql= """INSERT INTO timesheet(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode)
            VALUES( %s,%s, %s, %s, %s, %s, %s)"""
           cur.execute(sql, row)
           con.commit()
           i +=1 
    cur.close()
    con.close()


if __name__ == "__main__":
    extract_timesheet_data("../../data/timesheet_2021_06_23.csv")
    extract_timesheet_data("../../data/timesheet_2021_05_23.csv")
    extract_timesheet_data("../../data/timesheet_2021_07_24.csv")
