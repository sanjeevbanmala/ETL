def load_customer_data(cur,con):
    with open('../sql/load_customer.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def load_product_data(cur,con):
    with open('../sql/load_product.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def load_sale_data(cur,con):
    with open('../sql/load_sale.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

