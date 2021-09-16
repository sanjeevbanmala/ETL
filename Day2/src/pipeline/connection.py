import psycopg2

def connect():
    return psycopg2.connect( host = "localhost", database = "extraction_database", user ="postgres", password ="password", port =5432)
