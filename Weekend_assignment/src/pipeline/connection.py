import psycopg2

def connect():
    return psycopg2.connect( 
        host = "localhost", 
        database = "ecommerce_warehouse", 
        user ="postgres", 
        password ="password", 
        port =5432
        )
