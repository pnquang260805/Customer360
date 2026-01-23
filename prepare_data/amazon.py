from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from services.lazada_crawler import LazadaCrawler
from services.postgres_connector import PostgresConnector

def main():
    options = Options()
    user_agent = UserAgent(os=["Windows", "Linux", "Ubuntu", "Chrome OS", "Mac OS X"])
    agent = user_agent.random
    print(agent)
    options.add_argument("--headless=new")
    options.add_argument(f"user-agent={agent}")
    # options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-popup-blocking")
    # options.add_argument("--start-maximized")
    # options.add_experimental_option("detach", True)

    driver = webdriver.Chrome(options)

    crawler = LazadaCrawler(driver)
    data = crawler.crawl("https://pages.lazada.vn/wow/gcp/route/lazada/vn/upr_1000345_lazada/channel/vn/upr-router/vn?spm=a2o4n.tm80437192.6513940080.1.2124y3qiy3qi0Z&pha=true&hybrid=1&data_prefetch=true&prefetch_replace=1&at_iframe=1&wh_pid=/lazada/megascenario/vn/mega-tet-2026/more-lp-v2-1765989511487&trafficSource=TT&preModuleData=%7B%22__data_entityType%22%3A%22ITEM%22%2C%22__data_mName%22%3A%22lzdrwb-product3-in-a-row%22%2C%22__data_configId%22%3A%2228153324%22%2C%22__data_tppId%22%3A14053%2C%22__data_maxItemCount%22%3A%222000%22%7D")
    if len(data) == 0:
        print("Error")
        return

    username = "postgres"
    password = "postgres"
    host = "localhost"
    port = 5432
    db_name = "store"
    table_name = "product"
    
    connector = PostgresConnector(db_name, username, password, host, port)
    print("connected")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name}( 
        product_id SERIAL NOT NULL PRIMARY KEY,
        product_name TEXT,
        product_link TEXT,
        price BIGINT,
        base_price BIGINT,
        currency VARCHAR(10),
        sale_percents VARCHAR(50),
        product_type VARCHAR(100)
    )
    """
    curr = connector.cursor
    curr.execute(create_table_query)
    conn = connector.conn
    conn.commit()

    columns = ['product_name', 'product_link', 'price', "base_price", 'currency', 'sale_percents', 'product_type']
    for product in data:
        product_name = product.get("product_name", None)
        print(product_name)
        product_link = product.get("product_link", None)
        price = product.get("price", None)
        base_price = product.get("base_price", None)
        currency = product.get("currency", None)
        sale_percents = product.get("sale_percents", None)
        product_type = product.get("product_type", None)
        values = [product_name, product_link, price, base_price, currency, sale_percents, product_type]
        connector.insert(table_name, columns, values)

if __name__ == "__main__":
    main()