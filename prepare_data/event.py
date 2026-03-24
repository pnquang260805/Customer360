import random
import json
import time

from services.postgres_connector import PostgresConnector
from typing import *


def main():
    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = 5432
    db_name = "store"
    table_name = "event"
    connector = PostgresConnector(db_name, username, password, host, port)
    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            event_id BIGSERIAL PRIMARY KEY,
            type TEXT,
            client_id TEXT,
            user_id varchar(50),
            url TEXT,
            product_id INT
        )
    """
    connector.cursor.execute(query)
    connector.conn.commit()


if __name__ == "__main__":
    main()
