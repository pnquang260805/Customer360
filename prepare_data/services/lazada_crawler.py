import time
import random

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys


from bs4 import BeautifulSoup
from typing import List, Dict, Any

from common.logger import log, auto_log

class LazadaCrawler:
    def __init__(self, driver : webdriver.Chrome):
        self.driver = driver
        self.product_types = [
        "Electronics",
        "Computers & Accessories",
        "Smart Home",
        "Arts & Crafts",
        "Automotive",
        "Baby",
        "Beauty & Personal Care",
        "Women's Fashion",
        "Men's Fashion",
        "Health & Household",
        "Home & Kitchen",
        "Industrial & Scientific",
        "Luggage",
        "Pet Supplies",
        "Sports & Outdoors",
        "Tools & Home Improvement",
        "Toys & Games",
        "Video Games"
    ]

    @auto_log()
    def crawl(self, url : str) -> List[Dict[str, Any]]:
        # Entry
        self.driver.get(url)

        html = self.driver.find_element(By.TAG_NAME, 'html')
        for _ in range(50):
            time.sleep(1)
            html.send_keys(Keys.END)


        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        products_html = soup.find_all("a", {"class": "jfy-product-card-component-pc"})
        products = []
        for p in products_html:
            link = p.get('href', None)
            log.info(link)
            name = p.find("span", {"class": "product-card-title"})
            if name:
                name = name.get_text(strip=True)
            log.info(name)
            prices = p.find("span", {"class": "lzdPriceDiscountPCV2"})
            if prices:
                prices = int("".join(prices.get_text(strip=True).split(","))[1:] )
            base_prices = p.find("span", {"class": "lzdPriceOriginPCV2"})
            if base_prices:
                base_prices = int("".join(base_prices.get_text(strip=True).split(","))[1:])
            symbol = p.find("span", {"class": "lzdPriceOriginPCV2"})
            if symbol:
                symbol = symbol.get_text(strip=True)[0]
            log.info(symbol)
            data = {
                "product_link": link,
                "product_name": name,
                "price": prices,
                "base_price": base_prices,
                "currency": symbol,
                "product_type": random.choice(self.product_types)
            }
            products.append(data)
        self.driver.close()
        return products