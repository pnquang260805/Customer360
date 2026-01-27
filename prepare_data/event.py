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

fake = Faker()

def __gen_platform():
    platforms = ["iPhone", "Android", "Linux", "Windows", "MacOS"]
    return random.choice(platforms)

def __gen_action():
    actions = ["click", "add_to_cart"]
    return random.choice(actions)

def __gen_event_date(creation_date):
    c_date = datetime.strptime(creation_date, "%Y-%m-%d").date()
    return fake.date_time_between_dates(c_date, datetime.now())

def __gen_event_id():
    return str(uuid4())

def __gen_session_id():
    return str(uuid4())

def gen(customers, products):
    customer = random.choice(customers)
    product = random.choice(products)
    customer_id = customer[0]
    customer_creation_date = customer[1]
    return {
        "event_id": __gen_event_id(),
        "customer_id": customer_id,
        "platform": __gen_platform(),
        "action": __gen_action(),
        "session_id": __gen_session_id(),
        "event_time": __gen_event_date(customer_creation_date).isoformat(),
        "url": product
    }

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
    cursor.execute(f"SELECT product_id, product_link FROM {product_table}")
    temp= cursor.fetchall()
    products_link = [ids[1] for ids in temp] 
    return crm_data, products_link

def callback(err, msg):
    if err:
        log.error(f"Error when sending message to {msg.topic()}")
    else:
        log.info(f"Message sent to {msg.topic()}")

def send_msg(producer : Producer, topic : str, message: dict) -> None:
    producer.produce(topic, json.dumps(message), on_delivery=callback)
    producer.flush()

def main():
    TOPIC = "event-topic"
    BOOTSTRAP = "localhost:9092"
    conf = {
        "bootstrap.servers": BOOTSTRAP
    }
    producer = Producer(conf)
    crm_data, product_links = fetch_data()
    for i in range(100):
        send_msg(producer, TOPIC, gen(crm_data, product_links))
        time.sleep(random.random())

if __name__ == "__main__":
    main()