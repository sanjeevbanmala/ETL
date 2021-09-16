import psycopg2

def source_connect():
    return psycopg2.connect(host="localhost",user="postgres", database="ecommerce_source" ,password="password", port = 5432)

def dest_connect():
    return psycopg2.connect(host="localhost",user="postgres", database="ecommerce_destination" ,password="password", port = 5432)

def extract_sales_from_source():
    sconn=source_connect()
    dconn=dest_connect()

    scur=sconn.cursor()
    dcur=dconn.cursor()

    select_query="""
    select s.user_id, u.username, s.product_id, p.name as product_name, c.id as category_id, c.name as category_name,
    p.price as current_price, s.price as sold_price,s.quantity as sold_quantity,(p.quantity-s.quantity)as remaining_quantity,s.updated_at as sales_date
    from sales s
    inner join users u on s.user_id=u.id
    inner join products p on s.product_id = p.id
    inner join categories c on p.category_id = c.id;"""

    scur.execute(select_query)
    result=scur.fetchall()
    sql='''
    INSERT INTO raw_sales(user_id, username, product_id, product_name, category_id, category_name, current_price, sold_price, sold_quantity, remaining_quantity, sales_date)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    for row in result:
        dcur.execute(sql,row)
        dconn.commit()
    


    sconn.close()
    dconn.close()

if __name__ == "__main__":
    extract_sales_from_source()
