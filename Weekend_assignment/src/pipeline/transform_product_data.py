def transform_product_data(cur,con):
    with open('../sql/update_product.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()
