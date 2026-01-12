import time
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent

from services.bds_crawler import BDSCrawler
from services.mongo_connector import MongoConnector
from common.logger import log, auto_log

def main() -> None:
    # Base args
    url = 'https://batdongsan.com.vn/ban-can-ho-chung-cu-ha-noi'
    agent = UserAgent().random

    # Initiate instance
    ## Selenium
    options = Options()
    options.add_argument(f"user-agent={agent}")
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    crawler = BDSCrawler(options) 

    ## MongoDB
    mongo_host = "kubernetes.docker.internal"
    mongo_port = 27017
    username = "mongo"
    password = "mongo"
    connector = MongoConnector(mongo_host, mongo_port, username, password)
    connector.connect()
    
    db_name = "real_estate"
    collection_name = "bdsvn"

    # Exec
    raw_data = crawler.crawl(url)
    crawler.close()
    html = crawler.html_parser(raw_data)
    

    all_urls = crawler.get_all_url(html)
    for url in all_urls:
        sub_crawler = BDSCrawler(options=options)
        sub_data = sub_crawler.crawl(url)
        sub_crawler.close()
        bs = crawler.html_parser(sub_data)
        data = crawler.get_data(url, bs)
        connector.insert_one(db_name, collection_name, data)
        time.sleep(5)
    
    connector.disconnect()


if __name__ == "__main__":
    main()