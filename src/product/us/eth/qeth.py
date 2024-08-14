from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlite3 import Connection
from typing import Union


class QETH(ETP):
    """Invesco"""

    def url(self):
        return "https://www.invesco.com/us/financial-products/etfs/product-detail?ticker=QETH"

    def _file_extension(self):
        return "html"

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        options = Options()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options)
        driver.get(self.url())
        driver.find_element(By.ID, 'onetrust-accept-btn-handler').click()
        driver.find_element(By.CLASS_NAME, 'o-label__container').click()
        driver.get(self.url())

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
