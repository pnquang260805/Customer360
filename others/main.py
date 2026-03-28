import requests
import psycopg2


def setup_debezium():
    debezium_url = "http://debezium:8083/connectors"
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    DATABASE = "store"
    customer_connector_name = "customer-connector"
    product_connector_name = "product-connector"

    tables = [
        {"name": "customer", "key": "customer"},
        {"name": "product", "key": "product"},
        {"name": "transaction", "key": "transaction"},
        {"name": "event", "key": "event"},
    ]

    for table in tables:
        connector_name = f"{table['name']}-connector"

        config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "postgres",
                "database.port": "5432",
                "database.user": POSTGRES_USER,
                "database.password": POSTGRES_PASSWORD,
                "database.dbname": DATABASE,
                "database.server.name": "postgres_server",
                "table.include.list": f"public.{table['name']}",
                "plugin.name": "pgoutput",
                "slot.name": f"debezium_slot_{table['name']}",
                "publication.name": f"debezium_pub_{table['name']}",
                "topic.prefix": f"e-commerce-{table['key']}",  # 1 connector chỉ có 1 prefix
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "transforms": "AddStaticKey,ExtractStaticKey",
                "transforms.AddStaticKey.type": "org.apache.kafka.connect.transforms.InsertField$Key",
                "transforms.AddStaticKey.static.field": "temp_key",
                "transforms.AddStaticKey.static.value": table["key"],
                "transforms.ExtractStaticKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
                "transforms.ExtractStaticKey.field": "temp_key",
            },
        }
        requests.delete(f"{debezium_url}/{connector_name}")
        try:
            r = requests.post(debezium_url, json=config)
            if r.status_code == 201:
                print(f"Successfully created connector for {table['name']}")
            else:
                print(f"Failed to create {table['name']}: {r.text}")
        except Exception as e:
            print(f"Error connecting to Debezium: {e}")


def setup_postgres():
    username = "postgres"
    password = "postgres"
    host = "postgres"
    port = 5432
    db_name = "store"

    conn = psycopg2.connect(
        database=db_name, user=username, password=password, host=host, port=port
    )
    print("Connected")
    curr = conn.cursor()
    create_product_table = f"""
    CREATE TABLE IF NOT EXISTS product( 
        product_id SERIAL NOT NULL PRIMARY KEY,
        product_name TEXT,
        product_link TEXT,
        price DECIMAL(10,2),
        base_price DECIMAL(10, 2),
        currency VARCHAR(10),
        sale_percents VARCHAR(50),
        product_type VARCHAR(100)
    )
    """
    curr.execute(create_product_table)
    conn.commit()

    create_customer_table = f"""
    CREATE TABLE IF NOT EXISTS customer( 
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
    curr.execute(create_customer_table)
    conn.commit()

    create_transaction_table = """
        CREATE TABLE IF NOT EXISTS transaction(
            transaction_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT,
            price DECIMAL(10, 2),
            quantity INTEGER,
            total_amount DECIMAL(10, 2),
            event_time DATE
        )
    """
    curr.execute(create_transaction_table)
    conn.commit()

    query = f"""
        CREATE TABLE IF NOT EXISTS event(
            event_id BIGSERIAL PRIMARY KEY,
            type TEXT,
            customer_id varchar(50),
            url TEXT,
            time_stamp TIMESTAMPTZ DEFAULT NOW()
        )
    """
    curr.execute(query)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    try:
        setup_debezium()
        print("Done setup debezium")
    except Exception as e:
        print(f"Debezium: {e}")
    try:
        setup_postgres()
        print("Done setup postgres")
    except Exception as e:
        print(f"Postgres: {e}")
