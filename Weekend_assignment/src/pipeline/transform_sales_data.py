def transform_sales_data(cur,con):
    with open('../sql/update_sales.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()