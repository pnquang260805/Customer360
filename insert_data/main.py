import psycopg2
from faker import Faker
from typing import *
from uuid import uuid4
import random
from datetime import datetime
import json
from psycopg2.extras import execute_values

fake = Faker("vi_VN")

DB_NAME = "store"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

try:
    conn = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)
    print("Database connected successfully")
except:
    print("Database not connected successfully")

def __gender():
    return random.choice(["male", "female"])

def __gen_customer():
    d = {
        "customer_id": str(uuid4()), 
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": __gender(),
        "date_of_birth": fake.date_of_birth(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "country": "Vietnam",
        "creation_date": datetime.now().strftime("%Y-%m-%d")
    }
    return d

def insert_customer(n):
    query = f"""INSERT INTO customer
                    (customer_id, first_name, 
                    last_name, gender, date_of_birth, 
                    email, phone_number, address, country, 
                    creation_date)
                VALUES %s    
                """
    data = [__gen_customer() for _ in range(n)]
    execute_values(conn.cursor(), query, [tuple(d.values()) for d in data])
    conn.commit()

def insert_product(n):
    def gen_data():
        price = round(random.random() * random.randint(100, 500), 2)
        sale = round(random.random(), 4)
        return {
            "product_name": fake.text(max_nb_chars=50),
            "product_link": f"URL {random.randint(1, 10000)}",
            "price": price,
            "base_price": round(price + price*sale, 2),
            "currency": "USD",
            "sale_percents": round(sale * 100, 2),
            "product_type": random.choice(["Electronic", "Household"])
        }
    query = f"""INSERT INTO product
                    (product_name, product_link, 
                    price, base_price, currency, 
                    sale_percents, product_type)
                VALUES %s    
                """
    data = [gen_data() for _ in range(n)]
    execute_values(conn.cursor(), query, [tuple(d.values()) for d in data])
    conn.commit()

customer = int(input("Number of customer: "))
insert_customer(customer)
# product = int(input("Number of product: "))
# insert_product(product)