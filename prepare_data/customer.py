import random
from faker import Faker
from snowflake import SnowflakeGenerator
from datetime import datetime, timedelta

from services.postgres_connector import PostgresConnector

class Customer:
    def __init__(self):
        self.faker = Faker()

    def __gen_first_name(self):
        return self.faker.first_name()[:50]

    def __gen_last_name(self):
        return self.faker.last_name()[:50]

    def __gen_dob(self):
        return self.faker.date_of_birth(minimum_age=10, maximum_age=80).strftime("%Y-%m-%d")

    def __gen_gender(self):
        ran = random.randint(1,10000)
        if ran % 2 == 0:
            return "male"
        return "female"

    def __gen_address(self):
        return self.faker.address()

    def __gen_country(self):
        return self.faker.country()[:50]

    def __gen_email(self):
        return self.faker.email()[:50]

    def __gen_phone(self):
        return self.faker.phone_number()[:50]
    
    def __gen_active_date(self):
        return self.faker.date_this_decade().strftime("%Y-%m-%d")

    def generator(self):
        return {
            "first_name": self.__gen_first_name(),
            "last_name": self.__gen_last_name(),
            "gender": self.__gen_gender(),
            "date_of_birth": self.__gen_dob(),
            "email": self.__gen_email(),
            "phone_number": self.__gen_phone(),
            "address": self.__gen_address(),
            "country": self.__gen_country(),
            "active_date": self.__gen_active_date()
        }

def main() -> None:
    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = 5432
    db_name = "store"
    table_name = "customer"

    connector = PostgresConnector(db_name, username, password, host, port)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name}( 
        customer_id SERIAL NOT NULL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(10),
        date_of_birth DATE,
        email VARCHAR(50),
        phone_number VARCHAR(50),
        address TEXT,
        country VARCHAR(50),
        active_date DATE
    )
    """
    curr = connector.cursor
    curr.execute(create_table_query)
    conn = connector.conn
    conn.commit()

    for _ in range(random.randint(100, 500)):
        customers = Customer().generator()
        columns = list(customers.keys())
        values = list(customers.values())
        connector.insert(table_name, columns, values)
                
    connector.close()


if __name__ == "__main__":
    main()