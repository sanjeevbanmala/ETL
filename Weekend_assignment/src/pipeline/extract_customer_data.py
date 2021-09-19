from connection import connect


def extract_customer_data(cur,con,filePath):
    
    delete_sql = """DELETE FROM raw_customer"""
    cur.execute(delete_sql)
    con.commit()
    with open(filePath, 'r') as file:
        i = 0
        for line in file:
            if i == 0:
                i += 1
                continue
            row = line.split(",")
            sql = """INSERT INTO raw_customer(customer_id,user_name,first_name,last_name,country,town,active)
            VALUES( %s,%s, %s, %s, %s, %s, %s)"""
            cur.execute(sql, row)
            con.commit()
            i += 1

    search_sheet = "select customer_id from raw_customer_archive where sheet_name = '" + filePath + "'"
    cur.execute(search_sheet)
    if(cur.fetchall()):
        print("archive already exists for customers!!!")
    else:
        with open(filePath, 'r') as file:
            i = 0
            for line in file:
                if i == 0:
                    i += 1
                    continue
                row1 = line.split(",")
                row1.append(filePath)
                sql1 = """INSERT INTO raw_customer_archive(customer_id,user_name,first_name,last_name,country,town,active,sheet_name)

                VALUES( %s,%s, %s, %s, %s, %s, %s,%s)"""
                cur.execute(sql1, row1)
                con.commit()
                i += 1
        print("New archive created for customer data!!!!")
