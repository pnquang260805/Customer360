from fake_useragent import UserAgent
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

from services.mongo_connector import MongoConnector
from services.cellphones_crawler import CellphoneCrawler
from services.cellphones_extractor import CellphoneExtractor
from common.logger import log


def exec_crawl(url: str, collection_name) -> None:
    # Base args
    mongo_username = "mongo"
    mongo_password = "mongo"
    mongo_host = "kubernetes.docker.internal"
    mongo_port = 27017
    db_name = "products"

    user_agent = UserAgent()
    agent = user_agent.random

    options = Options()
    # options.add_argument(f"user-agent={agent}")
    # options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-popup-blocking")
    driver = webdriver.Chrome(options)

    # Setup services
    connector = MongoConnector(mongo_host, mongo_port, mongo_username, mongo_password)
    crawler = CellphoneCrawler(driver)
    extractor = CellphoneExtractor()

    crawler.set_wait_full_page(True)
    all_products = crawler.crawl(url)
    crawler.set_wait_full_page(False)

    bs_all_products = extractor.find_all_products(extractor.html_parser(all_products))
    for product in bs_all_products:
        detail_url = extractor.get_url(product)
        try:
            product_web_data = crawler.crawl(detail_url)
            extracted_data = extractor.extract(product_web_data)
            extracted_data["url"] = detail_url

            log.info(f"Done {detail_url}")
            connector.insert_one(db_name, collection_name, extracted_data)
        except Exception as e:
            log.error(f"{url} error: {e}")

    crawler.close()
    connector.disconnect()
