def transform_customer_data(cur,con):
    with open('../sql/update_customer.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()
