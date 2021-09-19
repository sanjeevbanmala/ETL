from connection import connect


def extract_sales_data(filePath):
    con = connect()
    cur = con.cursor()
    delete_sql = """DELETE FROM raw_sales"""
    cur.execute(delete_sql)
    con.commit()
    with open(filePath, 'r') as file:
        i = 0
        for line in file:
            if i == 0:
                i += 1
                continue
            row = line.split(",")
            sql = """INSERT INTO raw_sales(id,transaction_id,bill_no,bill_date,bill_location,customer_id,product_id,qty,uom,price,gross_price,tax_pc,tax_amt,discount_pc,discount_amt,net_bill_amt,created_by,updated_by,created_date,updated_date)
            VALUES( %s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s)"""
            cur.execute(sql, row)
            con.commit()
            i += 1

    search_sheet = "select id from raw_sales_archive where sheet_name = '" + filePath + "'"
    cur.execute(search_sheet)
    if(cur.fetchall()):
        print("archive already exists for sales!!!")
    else:
        with open(filePath, 'r') as file:
            i = 0
            for line in file:
                if i == 0:
                    i += 1
                    continue
                row1 = line.split(",")
                row1.append(filePath)
                sql1 = """INSERT INTO raw_sales_archive(id,transaction_id,bill_no,bill_date,bill_location,customer_id,product_id,qty,uom,price,gross_price,tax_pc,tax_amt,discount_pc,discount_amt,net_bill_amt,created_by,updated_by,created_date,updated_date,sheet_name)
                VALUES( %s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s,%s)"""
                cur.execute(sql1, row1)
                con.commit()
                i += 1
        print("New archive created for sales data!!!!")

    cur.close()
    con.close()
