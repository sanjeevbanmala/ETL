def load_product_data(cur,con):
    with open('../sql/load_product.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()
