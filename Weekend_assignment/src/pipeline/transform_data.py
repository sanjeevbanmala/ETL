def transform_customer_data(cur,con):
    with open('../sql/update_customer.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def transform_product_data(cur,con):
    with open('../sql/update_product.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()

def transform_sales_data(cur,con):
    with open('../sql/update_sales.sql') as file:
        sql = " ".join(file.readlines())
        cur.execute(sql)
        con.commit()