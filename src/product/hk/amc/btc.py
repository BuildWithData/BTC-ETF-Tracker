from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from time import sleep


class AB9042(ETP):
    """China AMC"""

    def url(self):
        return "https://www.chinaamc.com.hk/product/chinaamc-bitcoin-etf/"

    def _file_extension(self):
        return "html"

    def _xlsx_original_file_name(self):
        return "HKVAXBT_EN.xlsx"

    def _file_name(self, scrape_timestamp: datetime, type: str):
        out = f"{self.ticker}_{scrape_timestamp.isoformat(timespec='minutes')}"
        out += "." + type
        return out

    def scrape(self):

        timestamp = datetime.today()
        options = Options()

        options.add_experimental_option("prefs", {
                "download.default_directory": self.path(),
        })
#        options.add_argument('--headless')
        driver = webdriver.Chrome(options)

        driver.get(self.url())

        path = os.path.join(self.path(), self._file_name(timestamp, "html"))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(driver.page_source)

        holding_section = [s for s in driver.find_elements(By.TAG_NAME, 'section') if "holding details" in s.text.lower()][1]
        sleep(3)
        holding_section.find_element(By.TAG_NAME, "a").click()
        actual = Path(os.path.join(self.path(), self._xlsx_original_file_name()))

        sleep(2)
        if actual.exists() is False:
            raise RuntimeError(f"File {actual} does not exist")
        new = os.path.join(self.path(), self._file_name(timestamp, "xlsx"))
        actual.rename(new)

    def extract(self):
        raise NotImplementedError

    def update_db(self):
        raise NotImplementedError
