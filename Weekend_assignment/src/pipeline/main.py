from connection import connect
from extract_products_data import extract_products_data
from extract_sales_data import extract_sales_data
from extract_customer_data import extract_customer_data
from transform_customer_data import transform_customer_data
from transform_product_data import transform_product_data
from transform_sales_data import transform_sales_data
from load_customer_data import load_customer_data
from load_product_data import load_product_data
from load_sale_data import load_sale_data

def extract_raw_data():
    extract_sales_data(cur,con,'../../data/sales_dump.csv')
    extract_products_data(cur,con,'../../data/product_dump.csv')
    extract_customer_data(cur,con,'../../data/customer_dump.csv')
    

def transform_raw_data():
    transform_customer_data(cur,con)
    transform_product_data(cur,con)
    transform_sales_data(cur,con)

def load_transform_data():
    load_customer_data(cur,con)
    load_product_data(cur,con)
    load_sale_data(cur,con)

   



if __name__ == "__main__":
    con = connect()
    cur = con.cursor()
    extract_raw_data()
    transform_raw_data()
    load_transform_data()
    cur.close()
    con.close()
    
