from services.postgres_connector import PostgresConnector
from common.logger import log
from datetime import datetime


def gen_login(customer_id):
    return {
        
    }

def main():
    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = 5432
    db_name = "store"
    product_table = "product"
    customer_table = "customer"
    
    connector = PostgresConnector(db_name, username, password, host, port)
    print("connected")
    conn = connector.conn
    cursor = connector.cursor

    cursor.execute(f"SELECT customer_id FROM {customer_table}")
    customers_id = [ids[0] for ids in cursor.fetchall()]
    cursor.execute(f"SELECT product_id FROM {product_table}")
    products_id = [ids[0] for ids in cursor.fetchall()]
    
    



if __name__ == "__main__":
    main()