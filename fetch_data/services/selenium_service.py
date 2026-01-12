import time
from typing import *

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from interfaces.crawler import Crawler

class SeleniumService(Crawler):
    def __init__(self, options: Options) -> None:
        self.driver = webdriver.Chrome(options=options)
    
    def crawl(self, url : str) -> str:
        self.driver.get(url)
        return self.driver.page_source 
    
    def close(self):
        self.driver.close()