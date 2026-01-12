from bs4 import BeautifulSoup
from bs4.element import *
from typing import List

from selenium.webdriver.chrome.options import Options
from services.selenium_service import SeleniumService

from common.logger import log, auto_log

class BDSCrawler(SeleniumService):
    def __init__(self, options: Options) -> None:
        super().__init__(options)
        self.base_url = "https://batdongsan.com.vn"

    @auto_log()
    def html_parser(self, content : str) -> BeautifulSoup:
        return BeautifulSoup(content, "html.parser")

    @auto_log()
    def get_all_url(self, soup: BeautifulSoup) -> List[str]:
        a_tags = soup.find_all("a", attrs={"class": "re__unreport"})
        return [f"{self.base_url}/{tag['href']}" for tag in a_tags]
    
    @auto_log()
    def __get_title(self, content: BeautifulSoup) -> str:
        return content.find("div", attrs={"class": "re__address-title js__product-address"}).contents[0].strip()  # type: ignore
    
    @auto_log()
    def __get_description(self, content: BeautifulSoup) -> str:
        raw_description = content.find("div", attrs={"class": "re__detail-content"}).contents # type: ignore
        rows = [str(row).strip() for row in raw_description if not isinstance(row, Tag)]
        return "\n".join(rows)
    
    @auto_log()
    def __get_spec_info(self, content: BeautifulSoup) -> List[dict]:
        spec_info = content.find_all("div", attrs={"class":"re__pr-specs-content-item"})
        info = []
        for contents in spec_info:
            spec_title = contents.find("span", attrs={"class":'re__pr-specs-content-item-title'}).contents[0] # type: ignore
            spec_value = contents.find("span", attrs={"class":'re__pr-specs-content-item-value'}).contents[0] # type: ignore
            info.append({
                spec_title: spec_value # type: ignore
            })
        return info
    
    @auto_log()
    def __get_project_name(self, content: BeautifulSoup) -> str:
        return content.find("div", class_="re__project-title").contents[0] # type: ignore
    
    @auto_log()
    def __get_location(self, content: BeautifulSoup) -> str:
        return content.find("div", class_='re__address-title js__product-address').contents[0].strip() # type: ignore

    @auto_log()
    def __get_developer(self, content: BeautifulSoup) -> str:
        return content.find_all("span", class_="re__prj-card-config-value re__wrap-long-text")[-1].contents[-1].strip() # type: ignore
    
    @auto_log()
    def __get_project_info(self, content: BeautifulSoup) -> List[str]:
        product_info_ls = []
        product_info = content.find("ul", class_="re__product-info") # type: ignore
        for li in product_info.find_all("li"): # type: ignore
            product_title = li.find("span", class_="re__sp1").get_text(strip=True) # type: ignore
            value = li.find("span", class_="re__sp3").get_text(strip=True) # type: ignore
            product_info_ls.append({product_title:value})

        return product_info_ls

    @auto_log()   
    def get_data(self, url: str, content: BeautifulSoup) -> dict:
        try:
            return {
                "title": self.__get_title(content),
                "location": self.__get_location(content),
                "description": self.__get_description(content),
                "spec_info": self.__get_spec_info(content),
                "project_name": self.__get_project_name(content),
                "project_info": self.__get_project_info(content),
                "developer": self.__get_developer(content),
                "url": url,
            }
        except Exception as e:
            print(e)
            raise(e)