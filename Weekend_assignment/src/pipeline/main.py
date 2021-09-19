from extract_products_data import extract_products_data
from extract_sales_data import extract_sales_data
from extract_customer_data import extract_customer_data

def extract_raw_data():
    extract_sales_data('../../data/sales_dump.csv')
    extract_products_data('../../data/product_dump.csv')
    extract_customer_data('../../data/customer_dump.csv')



if __name__ == "__main__":
    extract_raw_data()
    
