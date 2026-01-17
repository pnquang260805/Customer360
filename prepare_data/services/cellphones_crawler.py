import time
from asyncio import wait

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By

from interfaces.crawler import Crawler
from common.logger import log, auto_log

class CellphoneCrawler(Crawler):
    def __init__(self, driver : webdriver.Chrome) -> None:
        self.driver = driver
        self.wait_full_page = False

    @auto_log()
    def __get_url(self, url : str) -> None:
        self.driver.get(url)

    @auto_log()
    def __get_full_page(self) -> None:
        while True:
            try:
                button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "button__show-more-product")))
                button.click()
                log.info("Click button")
                time.sleep(1)
            except (TimeoutException, StaleElementReferenceException):
                log.info("Done click button")
                break
            except Exception as e:
                log.error(e)
                raise e

    def set_wait_full_page(self, values : bool):
        self.wait_full_page = values

    @auto_log()
    def crawl(self, url : str) -> str:
        self.__get_url(url)
        if self.wait_full_page:
            self.__get_full_page()
        return self.driver.page_source

    def close(self):
        self.driver.close()