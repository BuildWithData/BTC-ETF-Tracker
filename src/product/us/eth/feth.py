from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlite3 import Connection
from time import sleep
from typing import Union


class FETH(ETP):
    """Fidelity"""

    def url(self):
        return "https://digital.fidelity.com/prgw/digital/research/quote/dashboard/summary?symbol=FETH"

    def _xls_original_file_name(self):
        return "documentExcel.xls"

    def _file_name(self, scrape_timestamp: datetime, type: str):
        out = f"{self.ticker}_{scrape_timestamp.isoformat(timespec='minutes')}"
        out += "." + self._file_extension(type)
        return out

    def _file_extension(self, type: str):
        # TODO: horrible
        out = {"html": "html", "xls": "xls"}
        return out[type]

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        options = Options()

        #options.add_argument('--headless') # TODO: not working

        options.add_experimental_option("prefs", {
                "download.default_directory": self.path(),
        })
        driver = webdriver.Chrome(options)
        driver.get(self.url())
        sleep(10)

        # TODO: error handling

        path = os.path.join(self.path(), self._file_name(timestamp, "html"))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(driver.page_source)

        driver.find_elements(By.CLASS_NAME, "pvd-link__link")[5].click()
        new_window = driver.window_handles[-1]
        driver.switch_to.window(new_window)
        sleep(10)
        driver.find_element(By.ID, "DALYTab").click()
        sleep(5)
        driver.find_element(By.ID, "fax_downloadexcel_link").click()

        actual = Path(os.path.join(self.path(), self._xls_original_file_name()))
        sleep(2)
        if actual.exists() is False:
            raise RuntimeError(f"File {actual} does not exist")
        new = os.path.join(self.path(), self._file_name(timestamp, "xls"))
        actual.rename(new)

    def extract(self):
        raise NotImplementedError

    def update_db(self, con: Connection) -> None:
        raise NotImplementedError
