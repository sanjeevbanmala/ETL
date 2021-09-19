from connection import connect


def extract_products_data(filePath):
    con = connect()
    cur = con.cursor()
    delete_sql = """DELETE FROM raw_products"""
    cur.execute(delete_sql)
    con.commit()
    with open(filePath, 'r') as file:
        i = 0
        for line in file:
            if i == 0:
                i += 1
                continue
            row = line.split(",")
            sql = """INSERT INTO raw_products(product_id,product_name,description,price,mrp,pieces_per_case,weight_per_piece,uom,brand,category,tax_percent,active,created_by,created_date,updated_by,updated_date)
            VALUES( %s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s,%s,%s)"""
            cur.execute(sql, row)
            con.commit()
            i += 1

    search_sheet = "select product_id from raw_products_archive where sheet_name = '" + filePath + "'"
    cur.execute(search_sheet)
    if(cur.fetchall()):
        print("archive already exists for products!!!")
    else:
        with open(filePath, 'r') as file:
            i = 0
            for line in file:
                if i == 0:
                    i += 1
                    continue
                row1 = line.split(",")
                row1.append(filePath)
                sql1 = """INSERT INTO raw_products_archive(product_id,product_name,description,price,mrp,pieces_per_case,weight_per_piece,uom,brand,category,tax_percent,active,created_by,created_date,updated_by,updated_date,sheet_name)

                VALUES( %s,%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s,%s,%s, %s)"""
                cur.execute(sql1, row1)
                con.commit()
                i += 1
        print("New archive created for product data!!!!")

    cur.close()
    con.close()
