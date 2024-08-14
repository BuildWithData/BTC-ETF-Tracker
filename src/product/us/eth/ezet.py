from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlite3 import Connection
import time
from typing import Union


class EZET(ETP):
    """Franklin Templeton"""

    def url(self):
        return "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/40521/SINGLCLASS/franklin-ethereum-etf/EZET"

    def _file_extension(self):
        return "html"

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        options = Options()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options)
        driver.get(self.url())
        time.sleep(10)
        driver.find_element(By.ID, 'onetrust-accept-btn-handler').click()

        # TODO: error handling

        path = os.path.join(self.path(), self._file_name(timestamp))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(driver.page_source)

    def extract(self):
        raise NotImplementedError

    def update_db(self, con: Connection) -> None:
        raise NotImplementedError
