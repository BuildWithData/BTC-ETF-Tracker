from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlite3 import Connection
from typing import Union
import os
import pandas as pd


class BTCO(ETP):
    """Invesco"""

    def url(self):
        return "https://www.invesco.com/us/financial-products/etfs/product-detail?ticker=BTCO"

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

        for name, content in self.files.items():
            t = BeautifulSoup(content, "html.parser")

            quick_facts = t.find_all(class_="widget gray-bg stacked canadian")[-1]

            ref_date = datetime.strptime(t.find(class_="asofdatetime").text.split(" ")[3].split("\t")[-1], "%m/%d/%Y").date().isoformat()
            market_cap = float(quick_facts.find_all(class_="pull-right")[-1].text.strip("$").strip("M"))
            daily_traded_volume = int(quick_facts.find_all(class_="pull-right")[4].text.split("\n")[1].split("\t")[-1].replace(",", ""))
            n_shares = int(float(quick_facts.find_all(class_="pull-right")[-2].text.strip("M")) * 10 ** 6)
            closing_price = float(t.find(class_="widget gray-bg stacked canadian").find(class_="pull-right").text.split("$")[-1])

            self.extracted[name] = {
                "file_name": name,
                "ref_date": ref_date,
                "market_cap": market_cap,
                "daily_traded_volume": daily_traded_volume,
                "n_shares": n_shares,
                "closing_price": closing_price,
                "n_coins": None,
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        table = "btco"
        keys = "ref_date"

        self._dump(df, table, keys, con)
