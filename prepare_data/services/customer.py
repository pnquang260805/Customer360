import random
from faker import Faker
from snowflake import SnowflakeGenerator

from services.mongo_connector import MongoConnector

class Customer:
    def __init__(self, instance: int):
        self.faker = Faker()
        self.instance = instance

    def __gen_customer_id(self) -> str:
        gen = SnowflakeGenerator(instance=self.instance)
        return str(next(gen))

    def __gen_first_name(self):
        return self.faker.first_name()

    def __gen_last_name(self):
        return self.faker.last_name()

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
        return self.faker.country()

    def generator(self):
        return {
            "customer_id": self.__gen_customer_id(),
            "first_name": self.__gen_first_name(),
            "last_name": self.__gen_last_name(),
            "gender": self.__gen_gender(),
            "dob": self.__gen_dob(),
            "address": self.__gen_address(),
            "country": self.__gen_country(),
        }

def main() -> None:
    mongo_username = "mongo"
    mongo_password = "mongo"
    mongo_host = "kubernetes.docker.internal"
    mongo_port = 27017
    db_name = "customers"
    collection_name = "customers"

    connector = MongoConnector(mongo_host, mongo_port, mongo_username, mongo_password)
    connector.connect()
    for _ in range(random.randint(1, 100)):
        customer = Customer(random.randint(1, 5))
        connector.insert_one(db_name, collection_name, customer.generator())

    connector.disconnect()


if __name__ == "__main__":
    main()