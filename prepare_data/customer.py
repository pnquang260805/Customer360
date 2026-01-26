import random
from faker import Faker
from snowflake import SnowflakeGenerator
from datetime import datetime, timedelta

from services.postgres_connector import PostgresConnector


class Customer:
    def __init__(self, locale):
        self.faker = Faker(locale)
        self.snowflake = SnowflakeGenerator(3)

    def __gen_id(self):
        return next(self.snowflake)

    def __gen_first_name(self):
        return self.faker.first_name()[:50]

    def __gen_last_name(self):
        return self.faker.last_name()[:50]

    def __gen_dob(self):
        return self.faker.date_of_birth(minimum_age=10, maximum_age=80).strftime(
            "%Y-%m-%d"
        )

    def __gen_gender(self):
        ran = random.randint(1, 10000)
        if ran % 2 == 0:
            return "male"
        return "female"

    def __gen_address(self):
        return self.faker.address()

    def __gen_email(self):
        return self.faker.email()[:50]

    def __gen_phone(self):
        return self.faker.phone_number()[:50]

    def __gen_creation_date(self):
        return self.faker.date_this_decade().strftime("%Y-%m-%d")

    def generator(self, country):
        return {
            "customer_id": self.__gen_id(),
            "first_name": self.__gen_first_name(),
            "last_name": self.__gen_last_name(),
            "gender": self.__gen_gender(),
            "date_of_birth": self.__gen_dob(),
            "email": self.__gen_email(),
            "phone_number": self.__gen_phone(),
            "address": self.__gen_address(),
            "country": country,
            "creation_date": self.__gen_creation_date(),
        }


def main() -> None:
    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = 5432
    db_name = "store"
    table_name = "customer"

    LOCALES = {
        "de_DE": "Germany",
        "fr_FR": "France",
        "ja_JP": "Japan",
        "vi_VN": "Vietnam",
    }

    connector = PostgresConnector(db_name, username, password, host, port)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name}( 
        customer_id VARCHAR(50) PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(10),
        date_of_birth DATE,
        email VARCHAR(50),
        phone_number VARCHAR(50),
        address TEXT,
        country VARCHAR(50),
        creation_date DATE
    )
    """
    curr = connector.cursor
    curr.execute(create_table_query)
    conn = connector.conn
    conn.commit()

    for _ in range(1000):
        (k, v) = random.choice(list(LOCALES.items()))

        customers = Customer(k).generator(v)
        columns = list(customers.keys())
        values = list(customers.values())
        connector.insert(table_name, columns, values)

    connector.close()


if __name__ == "__main__":
    main()
