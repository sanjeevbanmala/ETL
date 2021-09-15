import psycopg2

def connect():
    return psycopg2.connect( host = "localhost", database = "car_dealership", user ="postgres", password ="password", port =5432)
