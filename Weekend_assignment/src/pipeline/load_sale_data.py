def load_sale_data(cur,con):
    with open('../sql/load_sale.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()
