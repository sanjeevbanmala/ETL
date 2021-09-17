import psycopg2

def connect():
    return psycopg2.connect(
        host = "localhost", 
        database = "extraction_database", 
        user ="postgres", 
        password ="password", 
        port =5432
        )

def extract_employee_data(cur,con):
    delete="DELETE FROM raw_employee"
    cur.execute(delete)
    con.commit()
    with open('../sql/extract_employee.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def extract_timesheet_data(cur,con):
    delete="DELETE FROM raw_timesheet"
    cur.execute(delete)
    con.commit()
    with open('../sql/extract_timesheet.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def clean_hours_worked(cur,con):
    update_hours_worked="UPDATE raw_timesheet SET hours_worked = '0' WHERE paycode ='ABSENT';"
    cur.execute(update_hours_worked)
    con.commit()

def create_temporary_punch_details(cur,con):
    delete="DROP TABLE  IF EXISTS  temporary_punch_details ;"
    cur.execute(delete)
    con.commit()

    create_query='''CREATE TABLE temporary_punch_details AS
        SELECT DISTINCT(employee_id),punch_apply_date as shift_date, MIN(CAST (punch_in_time as TIMESTAMP)) as shift_start_time, 
        MAX(CAST(punch_out_time as TIMESTAMP)) as shift_end_time
        FROM raw_timesheet
        GROUP BY employee_id, punch_apply_date
        ORDER by 1 asc, 2 asc;'''
    cur.execute(create_query)
    con.commit()

def create_temporary_shift_time(cur,con):
    delete="DROP TABLE  IF EXISTS  temporary_shift_table ;"
    cur.execute(delete)
    con.commit()
    create_query="""
        CREATE TABLE temporary_shift_table as
        SELECT employee_id,shift_date,shift_start_time,shift_end_time,
        CASE 
        WHEN shift_start_time :: TIMESTAMP :: time BETWEEN '05:00' AND '11:00':: TIME 
        THEN 'Morning'
        WHEN shift_start_time :: TIMESTAMP :: time >= '12:00':: TIME
        THEN 'Evening'
        ELSE NULL
        END as shift_type 
        FROM temporary_punch_details;"""
    cur.execute(create_query)
    con.commit()

def create_temporary_tables_for_hours_worked(cur,con):
    delete="DROP TABLE  IF EXISTS  temporary_hours_table ;"
    delete2="DROP TABLE  IF EXISTS  temporary_ahours_table;"
    cur.execute(delete)
    cur.execute(delete2)
    con.commit()

    #temporary table for hours worked as paycode =WRK
    create_query="""
    CREATE TABLE temporary_hours_table as
    SELECT DISTINCT(employee_id),punch_apply_date, SUM(CAST(hours_worked as NUMERIC )) as hours_worked
    FROM raw_timesheet 
    WHERE paycode ='WRK'
    GROUP BY employee_id, punch_apply_date,paycode
    ORDER BY 1 asc, 2 asc;
    """
    #temporary table for hours worked as paycode =ABSENT
    create_query2="""
    CREATE TABLE temporary_ahours_table as
    SELECT DISTINCT(employee_id),punch_apply_date, SUM(CAST(hours_worked as NUMERIC )) as hours_worked
    FROM raw_timesheet 
    WHERE paycode ='ABSENT'
    GROUP BY employee_id, punch_apply_date,paycode
    ORDER BY 1 asc, 2 asc;
    """
    cur.execute(create_query)
    cur.execute(create_query2)
    con.commit()

def create_table_add_hours(cur,con):
    delete="DROP TABLE  IF EXISTS  temporary_select_hours_worked ;"
    delete2="DROP TABLE  IF EXISTS  temporary_select_ahours_worked;"
    cur.execute(delete)
    cur.execute(delete2)
    con.commit()
    create_query="""
    CREATE TABLE temporary_select_hours_worked AS
    SELECT s.employee_id,s.shift_date,s.shift_start_time,s.shift_end_time,s.shift_type,h.hours_worked 
    FROM temporary_hours_table h
    INNER JOIN temporary_shift_table s ON h.employee_id =s.employee_id AND h.punch_apply_date=s.shift_date;
    """
    create_query2="""
    CREATE TABLE temporary_select_ahours_worked AS
    SELECT s.employee_id,s.shift_date,s.shift_start_time,s.shift_end_time,s.shift_type,ah.hours_worked 
    FROM temporary_ahours_table ah
    INNER JOIN temporary_shift_table s ON ah.employee_id =s.employee_id and ah.punch_apply_date=s.shift_date;
    """
    cur.execute(create_query)
    cur.execute(create_query2)
    con.commit()

def insert_data_to_one_table(cur,con):
    insert_query="""
    INSERT INTO temporary_select_ahours_worked  
    SELECT * from  temporary_select_hours_worked;
    """
    cur.execute(insert_query)
    con.commit()

def create_attendance_record(cur,con):
    delete="DROP TABLE IF EXISTS temporary_attendance"
    cur.execute(delete)
    con.commit()
    create_query="""
    CREATE TABLE  temporary_attendance AS
    SELECT DISTINCT(employee_id),   punch_apply_date  ,
    CASE WHEN paycode='ABSENT' THEN 'ABSENT' ELSE 'PRESENT' END AS attendance
    FROM raw_timesheet
    GROUP BY employee_id, punch_apply_date,paycode
    ORDER BY 1 asc, 2 asc;
    """
    cur.execute(create_query)
    con.commit()

def create_full_attendance(cur, con):
    delete="DROP TABLE IF EXISTS temporary_attendance_table;"
    cur.execute(delete)
    con.commit()
    create_query ="""
    CREATE TABLE temporary_attendance_table AS
    SELECT h.employee_id, h.shift_date, h.shift_start_time, h.shift_end_time, h.shift_type, h.hours_worked, a.attendance
    FROM temporary_select_ahours_worked h
    INNER JOIN temporary_attendance a ON a.employee_id=h.employee_id AND a.punch_apply_date= h.shift_date;
    """
    cur.execute(create_query)
    con.commit()

def create_temporary_break(cur,con):
    delete="DROP TABLE IF EXISTS temporary_break;"
    cur.execute(delete)
    con.commit()
    create_query="""
    CREATE TABLE temporary_break AS
    SELECT DISTINCT(employee_id),  punch_apply_date, 'true' AS has_taken_break,
    SUM(CAST (hours_worked as FLOAT)) AS break_hour
    FROM raw_timesheet
    WHERE paycode ='BREAK'
    GROUP BY employee_id, punch_apply_date,paycode
    ORDER BY  1 ASC, 2 ASC;
    """
    cur.execute(create_query)
    con.commit()

def update_table_with_break(cur,con):
    alter1="ALTER TABLE temporary_attendance_table ADD has_taken_break BOOL DEFAULT 'false';"
    alter2="ALTER TABLE temporary_attendance_table ADD break_hour FLOAT DEFAULT 0;"
    cur.execute(alter1)
    cur.execute(alter2)
    con.commit()

    update="""
    UPDATE temporary_attendance_table ta
    SET has_taken_break = 'true'
    FROM temporary_break b
    WHERE b.employee_id=ta.employee_id AND b.punch_apply_date =ta.shifT_date;
    """
    update2="""
    UPDATE temporary_attendance_table ta
    SET break_hour = b.break_hour
    FROM temporary_break b
    WHERE b.employee_id=ta.employee_id AND b.punch_apply_date =ta.shifT_date;
    """
    cur.execute(update)
    cur.execute(update2)
    con.commit()

def create_temporary_charge(cur,con):
    delete="DROP TABLE IF EXISTS temporary_charge;"
    cur.execute(delete)
    con.commit()
    create_query="""
    CREATE TABLE temporary_charge AS
    SELECT DISTINCT(employee_id),  punch_apply_date, 'true' AS was_charge,
    SUM(CAST (hours_worked as FLOAT)) AS charge_hour
    FROM raw_timesheet
    WHERE paycode ='CHARGE'
    GROUP BY employee_id, punch_apply_date,paycode
    ORDER BY 1 asc, 2 asc;
    """
    cur.execute(create_query)
    con.commit()

def update_table_with_charge(cur,con):
    alter1="ALTER TABLE temporary_attendance_table ADD was_charge BOOL DEFAULT 'false';"
    alter2="ALTER TABLE temporary_attendance_table ADD charge_hour FLOAT DEFAULT 0;"
    cur.execute(alter1)
    cur.execute(alter2)
    con.commit()

    update="""
    UPDATE temporary_attendance_table ta
    SET was_charge = 'true'
    FROM temporary_charge b
    WHERE b.employee_id=ta.employee_id AND b.punch_apply_date =ta.shifT_date;
    """
    update2="""
    UPDATE temporary_attendance_table ta
    SET charge_hour = b.charge_hour
    FROM temporary_charge b
    WHERE b.employee_id=ta.employee_id AND b.punch_apply_date =ta.shifT_date;
    """
    cur.execute(update)
    cur.execute(update2)
    con.commit()

def create_temporary_on_call(cur,con):
    delete="DROP TABLE IF EXISTS temporary_on_call "
    cur.execute(delete)
    con.commit()

    create_query="""
    CREATE TABLE temporary_on_call AS
    SELECT DISTINCT(employee_id),  punch_apply_date, 'true' AS was_on_call,
    SUM(CAST (hours_worked AS FLOAT)) AS on_call_hour
    FROM raw_timesheet
    WHERE paycode ='ON_CALL'
    GROUP BY employee_id, punch_apply_date,paycode
    ORDER by 1 ASC, 2 ASC;
    """
    cur.execute(create_query)
    con.commit()

def update_table_with_on_call(cur,con):
    alter1="ALTER TABLE temporary_attendance_table ADD was_on_call  BOOL DEFAULT 'false';"
    alter2="ALTER TABLE temporary_attendance_table ADD on_call_hour  FLOAT DEFAULT 0;"
    cur.execute(alter1)
    cur.execute(alter2)
    con.commit()
    update="""
    UPDATE temporary_attendance_table ta
    SET was_on_call = 'true'
    FROM temporary_on_call b
    WHERE b.employee_id=ta.employee_id AND b.punch_apply_date =ta.shifT_date;
    """
    update2="""
    UPDATE temporary_attendance_table ta
    SET on_call_hour = b.on_call_hour
    FROM temporary_on_call b
    WHERE b.employee_id=ta.employee_id AND b.punch_apply_date =ta.shifT_date;
    """
    cur.execute(update)
    cur.execute(update2)
    con.commit()

def add_department_id(cur,con):
    alter="ALTER TABLE temporary_attendance_table ADD department_id VARCHAR(250);"  
    cur.execute(alter)
    con.commit()
    update="""
    UPDATE temporary_attendance_table ta
    SET department_id = e.department_id
    FROM raw_employee e
    WHERE e.employee_id = ta.employee_id;
    """
    cur.execute(update)
    con.commit()

def create_absent_count(cur,con):
    delete="DROP TABLE IF EXISTS temporary_absent_count;"
    cur.execute(delete)
    con.commit()
    create="""
    CREATE TABLE temporary_absent_count AS 
    SELECT shift_date,department_id,COUNT(attendance)
    FROM temporary_attendance_table
    GROUP by shift_date,department_id
    ORDER by 1 ASC, 2 ASC;
    """
    cur.execute(create)
    con.commit()
   
def update_table_with_absent_count(cur,con):
    alter="ALTER TABLE temporary_attendance_table ADD num_teammates_absent NUMERIC DEFAULT 0;"
    cur.execute(alter)
    con.commit()
    update="""
    UPDATE temporary_attendance_table ta
    SET num_teammates_absent= ac.count
    FROM temporary_absent_count ac
    WHERE ac.department_id=ta.department_id AND ac.shift_date=ta.shift_date;
    """
    cur.execute(update)
    con.commit()

def main():
    con =connect()
    cur=con.cursor()
    extract_employee_data(cur,con)
    extract_timesheet_data(cur,con)
    clean_hours_worked(cur,con)
    create_temporary_punch_details(cur,con)
    create_temporary_shift_time(cur,con)
    create_temporary_tables_for_hours_worked(cur,con)
    create_table_add_hours(cur,con)
    insert_data_to_one_table(cur,con)
    create_attendance_record(cur,con)
    create_full_attendance(cur, con)
    create_temporary_break(cur,con)
    update_table_with_break(cur,con)
    create_temporary_charge(cur,con)
    update_table_with_charge(cur,con)
    create_temporary_on_call(cur,con)
    update_table_with_on_call(cur,con)
    add_department_id(cur,con)
    create_absent_count(cur,con)
    update_table_with_absent_count(cur,con)



if __name__ == "__main__":
    main()