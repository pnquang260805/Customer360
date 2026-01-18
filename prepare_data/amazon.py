from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from services.AmazonCrawler import AmazonCrawler
from services.mongo_connector import MongoConnector

if __name__ == "__main__":
    options = Options()
    user_agent = UserAgent(os=["Windows", "Linux", "Ubuntu", "Chrome OS", "Mac OS X"])
    agent = user_agent.random
    print(agent)
    # options.add_argument("--headless=new")
    options.add_argument(f"user-agent={agent}")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--start-maximized")
    # options.add_experimental_option("detach", True)

    driver = webdriver.Chrome(options)

    crawler = AmazonCrawler(driver)
    data = crawler.crawl("https://www.amazon.com/")

    mongo_username = "mongo"
    mongo_password = "mongo"
    mongo_host = "kubernetes.docker.internal"
    mongo_port = 27017
    db_name = "products"
    connector = MongoConnector(mongo_host, mongo_port, mongo_username, mongo_password)
    connector.insert_many(db_name, "products", data)
    connector.disconnect()