from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from time import sleep


class BE9009(ETP):
    """BOSERA"""

    def url(self, type_: str = None):
        out = {
            "html": "http://www.bosera.com.hk/en-US/products/fund/detail/ETHL",
            "api": "http://www.bosera.com.hk/api/fundinfo/funddetail.json"
        }

        if type_ is not None:
            out = out[type_]

        return out

    def _file_extension(self):
        return "html"

    def _xlsx_original_file_name(self):
        return "ETHL_AllHoldings.xlsx.crdownload"

    def _file_name(self, scrape_timestamp: datetime, type_: str):
        out = f"{self.ticker}_{scrape_timestamp.isoformat(timespec='minutes')}"
        out += "." + type_
        return out

    def scrape(self):

        timestamp = datetime.today()
        options = Options()

        options.add_experimental_option("prefs", {
                "download.default_directory": self.path(),
        })
#        options.add_argument('--headless')
        options.add_argument("--disable-features=InsecureDownloadWarnings")
        driver = webdriver.Chrome(options)

        driver.get(self.url("html"))
        sleep(5) # first time always fails to load the webpage
        driver.get(self.url("html"))
        sleep(5)

        driver.execute_script("window.scrollTo(0, 3500)")
        driver.find_element(By.CLASS_NAME, 'tos-modal_button-group').find_elements(By.TAG_NAME, "span")[1].click()

        section_tab = driver.find_element(By.CLASS_NAME, 'bs-tab-list')
        holdings = [s for s in section_tab.find_elements(By.TAG_NAME, "li") if "holding" in s.text.lower()][0]
        holdings.click()

        driver.execute_script("window.scrollTo(0, 500)")
        sleep(5)
        download_button = [a for a in driver.find_elements(By.TAG_NAME, "a") if "full holdings details" in a.text.lower()][0]
        download_button.click()

        actual = Path(os.path.join(self.path(), self._xlsx_original_file_name()))

        sleep(2)
        if actual.exists() is False:
            raise RuntimeError(f"File {actual} does not exist")
        new = os.path.join(self.path(), self._file_name(timestamp, "xlsx"))
        actual.rename(new)

        # when scraping webpage only javascript code is returned
        # let's get data from API
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US',
            'Referer': 'http://www.bosera.com.hk/en-US/products/fund/detail/ETHL',
        }

        params = {
            'fundCode': 'ETHL',
        }

        fund_information = requests.get(
            self.url("api"),
            params=params,
            headers=headers,
            verify=False,
        )

        fund_information.raise_for_status()

        path = os.path.join(self.path(), self._file_name(timestamp, "json"))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(fund_information.text)

    def extract(self):
        raise NotImplementedError

    def update_db(self):
        raise NotImplementedError
