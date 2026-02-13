import random
import json
import time

from services.postgres_connector import PostgresConnector
from common.logger import log
from datetime import datetime, timedelta
from uuid import uuid4
from faker import Faker
from confluent_kafka import Producer
from typing import *

from services.kafka_service import KafkaService

fake = Faker()

def __gen_event_date(creation_date):
    c_date = datetime.strptime(creation_date, "%Y-%m-%d").date()
    return fake.date_time_between_dates(c_date, datetime.now())

def __gen_id():
    return str(uuid4())

def __noise_calculate(a : int, b : int) -> int:
    choice = random.random()
    if choice >= 0.05: 
        return a * b
    else: # 5% tỷ lệ xảy ra lỗi
        return a * b * random.random()
    
def __noise_data(a : int) -> int:
    choice = random.random()
    if choice >= 0.05:
        return a
    else:
        return a * random.random()

def fetch_data():
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

    cursor.execute(f"SELECT customer_id, creation_date FROM {customer_table}")
    temp = cursor.fetchall()
    crm_ids = [ids[0] for ids in temp]
    creation_dates = [ids[1].strftime("%Y-%m-%d") for ids in temp]
    crm_data = list(zip(crm_ids, creation_dates))
    cursor.execute(f"SELECT product_id, product_name, price FROM {product_table}")
    temp = cursor.fetchall()
    products = []
    for product in temp:
        products.append({
            "product_id": product[0],
            "product_name": product[1],
            "price": product[2], # Sẽ có validate dữ liệu product trong silver
        })
    return crm_data, products

def gen(customers, products : List[dict]):
    customer = random.choice(customers)
    product = random.choice(products)
    customer_id = customer[0]
    customer_creation_date = customer[1]
    quantity = random.randint(1, 50)
    return {
        "transaction_id": __gen_id(),
        "customer_id": customer_id,
        "product_id": product.get("product_id", None),
        "product_name": product.get("product_name", None),
        "price": __noise_data(product.get("price")),
        "quantity": quantity,
        "total_amount": __noise_calculate(product.get("price"), quantity),
        "event_time": __gen_event_date(customer_creation_date).isoformat(),
    }

def main():
    TOPIC = "raw-2-bronze-topic"
    
    crm_data, product_links = fetch_data()
    kafka_service = KafkaService()
    for i in range(random.randint(10, 50)):
        kafka_service.send_msg(TOPIC, gen(crm_data, product_links), key="transaction")
        time.sleep(random.random()*1.5)

if __name__ == "__main__":
    main()