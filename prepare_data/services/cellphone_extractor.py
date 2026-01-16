from datetime import datetime
from bs4 import BeautifulSoup, ResultSet

from common.logger import log, auto_log
from interfaces.extractor import Extractor

class CellphoneExtractor(Extractor):
    def html_parser(self, content : str) -> BeautifulSoup:
        return BeautifulSoup(content, "html.parser")

    @auto_log()
    def find_all_products(self, content : BeautifulSoup) -> ResultSet:
        return content.find_all("a", {"class" : "product__link"})

    @auto_log()
    def get_url(self, content : BeautifulSoup) -> str:
        return content['href']

    @auto_log()
    def __get_product_name(self, content : BeautifulSoup) -> str:
        div = content.find("div", attrs={"class": "box-product-name"})
        return div.find("h1").get_text(strip=True)

    @auto_log()
    def __get_prices(self, content : BeautifulSoup) -> dict[str, str]:
        sale_price = content.find("div", {"class": "sale-price"}).get_text(strip=True)
        prices = {"price": sale_price}
        bs_base_price = content.find("del", {"class": "base-price"})
        if bs_base_price:
            base_price = bs_base_price.get_text(strip=True)
            prices["base_price"] = base_price
        return prices

    @auto_log()
    def __get_spec(self, content : BeautifulSoup) -> dict[str, str]:
        table = content.find("table", attrs={"class": "technical-content"})
        spec = {}
        tr_tags = table.find_all("tr", {"class": "technical-content-item"})
        for row in tr_tags:
            key = row.find("td").get_text(strip=True)
            value = row.find("p").get_text(strip=True)
            spec[key] = value
        return spec

    @auto_log()
    def extract(self, content : str):
        bs_content = self.html_parser(content)
        # url = self.get_url(bs_content)
        product_name = self.__get_product_name(bs_content)
        product_price = self.__get_prices(bs_content)
        spec = self.__get_spec(bs_content)

        today = datetime.now().strftime("%Y-%m-%d")
        return {
            "product_name": product_name,
            "product_prices": product_price,
            "spec": spec,
            "craw_date": today,
        }