import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from common.logger import log

class AmazonCrawler:
    def __init__(self, driver : webdriver.Chrome):
        self.driver = driver

    def crawl(self, url : str):
        # Entry
        self.driver.get(url)

        # Wait
        # //*[@id="nav-xshop"]/ul/li[1]/div/a
        WebDriverWait(self.driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="nav-xshop"]/ul/li[1]/div/a')))
        button = self.driver.find_element(By.XPATH, '//*[@id="nav-xshop"]/ul/li[1]/div/a')
        # button.click()
        """
        self.driver.execute_script(...):
        Đây là hàm cho phép Selenium chạy trực tiếp một đoạn mã JavaScript bên trong trình duyệt 
        (giống như bạn mở Console của trình duyệt F12 và gõ code vào đó).
        "arguments[0].click();":
        Đây là đoạn code JavaScript được chạy.
        arguments là một danh sách (mảng) chứa các biến được truyền từ Python vào JavaScript.
        arguments[0] đại diện cho phần tử đầu tiên bạn truyền vào (chính là biến button).
        .click() là hàm thuần của JavaScript để kích hoạt sự kiện click.
        button:
        Đây là đối tượng Web Element của Selenium mà bạn đã tìm thấy trước đó. 
        Selenium sẽ tự động chuyển đổi nó thành một phần tử HTML (DOM element) để JavaScript có thể hiểu được.
        """
        self.driver.execute_script("arguments[0].click();", button)

        current_position = 0
        total_height = self.driver.execute_script("return document.body.scrollHeight")
        while current_position < total_height:
            current_position += 500
            time.sleep(1.5)
            self.driver.execute_script(f"window.scrollTo(0, {current_position})")
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            try: # ButtonLink-module__root_eh2cgp8M2THLjamsgeRE
                more_deals_button = self.driver.find_element(By.CLASS_NAME, "ButtonLink-module__root_eh2cgp8M2THLjamsgeRE")
                self.driver.execute_script("arguments[0].click()", more_deals_button)
            except Exception as e:
                time.sleep(1.5)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        products_html = soup.find_all("div", {"data-testid": "product-card"})
        products = []
        for p in products_html:
            link = p.find("a", {"data-testid": "product-card-link"}).get("href")
            log.info(link)
            name = p.find("span", {"class": "a-truncate-full"}).get_text(strip=True)
            log.info(name)
            prices = p.find("span", {"aria-hidden": "true"})
            symbol = prices.find("span" ,{"class": "a-price-symbol"}).get_text(strip=True)
            log.info(symbol)
            price_whole = prices.find("span", {"class": "a-price-whole"}).get_text(strip=True)
            log.info(price_whole)
            data = {
                "product_link": link,
                "product_name": name,
                "price": price_whole,
                "currency": symbol
            }
            base_price_tag = p.find("span", {"class": "a-price a-text-price"}).find("span", {"class": "a-offscreen"})
            if base_price_tag:
                base_price = base_price_tag.get_text(strip=True)
                data["base_price"] = base_price
            sales_tag = p.find("span", {"class": "a-size-mini"})
            if sales_tag:
                sale_percents = sales_tag.get_text(strip=True)
                data["sale_percents"] = sale_percents
            products.append(data)
        self.driver.close()
        return products