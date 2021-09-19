def load_customer_data(cur,con):
    with open('../sql/load_customer.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()
