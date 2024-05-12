from bs4 import BeautifulSoup
from datetime import datetime
import os
import pandas as pd
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlite3 import Connection
from time import sleep
from utils.constants import MONTH2NUMBER


class HE9179(ETP):
    """Harvest Global Investments"""

    def url(self):
        return "https://www.harvestglobal.com.hk"

    def _file_extension(self):
        return "html"

    def scrape(self):

        timestamp = datetime.today()

        options = Options()
        #options.add_argument('--headless')
        driver = webdriver.Chrome(options)

        driver.get(self.url())
        sleep(10)
        driver.find_elements(By.CLASS_NAME, "button-box")[6].click()

        sleep(5)
        driver.execute_script("window.scrollTo(0, 1200)")
        sleep(3)
        driver.find_element(By.ID, "Our-Strategies").find_element(By.CLASS_NAME, "button-box").click()

        sleep(5)
        driver.execute_script("window.scrollTo(0, 1000)")
        sleep(3)
        driver.find_element(By.NAME, 'Accept').click()

        sleep(5)
        driver.find_elements(By.CLASS_NAME, "row")[0].find_elements(By.TAG_NAME, "span")[3].click()

        sleep(5)
        sleep(5)
        section = driver.find_element(By.ID, "fundvirtualasset-etf").find_element(By.XPATH, '..')
        section.find_elements(By.TAG_NAME, "span")[13].click()

        sleep(5)
        driver.execute_script("window.scrollTo(0, 1500)")
        market_information = [t for t in driver.find_elements(By.TAG_NAME, 'h3') if "market information" in t.text.lower()][0]
        usd_button = [s for s in market_information.find_elements(By.TAG_NAME, "span") if "USD" in s.text][0]
        usd_button.click()

        sleep(5)
        path = os.path.join(self.path(), self._file_name(timestamp))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(driver.page_source)

    def extract(self):

        for fn, content in self.files.items():
            t = BeautifulSoup(content, "html.parser")

            asset_allocation = t.find(id="holdings")

            ref_date = asset_allocation.find(class_="titAsof ng-binding").text[6:].strip()

            for name, number in MONTH2NUMBER.items():
                ref_date = ref_date.upper().replace(name, number)

            ref_date = list(reversed(ref_date.split(" ")))
            ref_date[-1] = "0" + ref_date[-1] if int(ref_date[-1]) < 10 else ref_date[-1]
            ref_date = "-".join(ref_date)

            weight_virtual_asset = float(
                asset_allocation
                .find(class_="holdingsAssetAllocation")
                .find(class_="ng-binding")
                .text.strip("%")
            )

            fund_size = float(
                asset_allocation
                .find(class_="holdingsAssetAllocation")
                .find(class_="ng-binding ng-scope")
                .text
            )
            fund_size = fund_size * 10 ** 6

            self.extracted[fn] = {
                "file_name": fn,
                "ref_date": ref_date,
                "weight_virtual_asset": weight_virtual_asset,
                "fund_size": fund_size
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        table = "he9179"
        keys = "ref_date"

        self._dump(df, table, keys, con)
