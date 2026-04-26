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
cur = conn.cursor()
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
    execute_values(cur, query, [tuple(d.values()) for d in data])
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
    execute_values(cur, query, [tuple(d.values()) for d in data])
    conn.commit()


def insert_transaction(n):
    query = f"""
        SELECT customer_id, phone_number from customer
    """
    cur.execute(query)
    customers = [{"customer_id":x[0], "phone_number": x[1]} for x in cur.fetchall()]
    cur.execute("""
        SELECT product_id, product_name, price
        FROM product
    """)
    products = [{"product_id": x[0], "product_name": x[1], "price": x[2]} for x in cur.fetchall()]

    def gen_data():
        i = random.randint(0, 2)
        
        # --- LUÔN CHỌN SẢN PHẨM TRƯỚC ---
        p = random.choice(products)
        p_id = p["product_id"]
        p_name = p["product_name"]
        price = float(p["price"])
        
        quantity = random.randint(1, 5)
        total_amount = round(price * quantity, 2)
        tx_id = str(uuid4())

        if i == 1: # Khách mua online (Web)
            c = random.choice(customers)
            return {
                "transaction_id": tx_id,
                "customer_id": c["customer_id"],
                "phone_number": c["phone_number"],
                "product_id": p_id,      # Lấy từ p
                "product_name": p_name,  # Lấy từ p
                "price": price,
                "quantity": quantity,
                "total_amount": total_amount,
                "source": "web"
            }
            
        elif i == 2: # Khách mua tại cửa hàng (Chưa có trong DB)
            return {
                "transaction_id": tx_id,
                "customer_id": str(uuid4()),
                "phone_number": fake.phone_number(),
                "product_id": p_id,      # Lấy từ p
                "product_name": p_name,  # Lấy từ p
                "price": price,
                "quantity": quantity,
                "total_amount": total_amount,
                "source": "store"
            }
            
        else: # Khách mua tại cửa hàng (Đã có trong DB)
            c = random.choice(customers)
            return {
                "transaction_id": tx_id,
                "customer_id": None,     # Để None để Spark xử lý join/fill sau
                "phone_number": c["phone_number"],
                "product_id": p_id,      # Lấy từ p
                "product_name": p_name,  # Lấy từ p
                "price": price,
                "quantity": quantity,
                "total_amount": total_amount,
                "source": "store"
            }
    
    data = [gen_data() for _ in range(n)]
    insert = f"""
                INSERT INTO transaction
                    (transaction_id, customer_id, 
                    phone_number, product_id, product_name, 
                    price, quantity, total_amount, source)
                VALUES %s
                """
    execute_values(cur, insert, [tuple(d.values()) for d in data])
    conn.commit()

def insert_event(n):
    q = f"""
        SELECT customer_id, phone_number from customer
    """
    cur.execute(q)
    customers_id = [x[0]for x in cur.fetchall()]
    def gen_data():
        price = round(random.random() * random.randint(100, 500), 2)
        sale = round(random.random(), 4)
        return {
            "type": random.choice(['view', 'add to cart']),
            "customer_id": random.choice(customers_id),
            "url": None,
        }
    query = f"""INSERT INTO event
                    (type, customer_id, 
                    url)
                VALUES %s    
                """
    data = [gen_data() for _ in range(n)]
    execute_values(cur, query, [tuple(d.values()) for d in data])
    conn.commit()


customer = int(input("Number of customer: "))
insert_customer(customer)
product = int(input("Number of product: "))
insert_product(product)
transaction = int(input("Number of transaction: "))
insert_transaction(transaction)
event = int(input("Number of event: "))
insert_event(event)