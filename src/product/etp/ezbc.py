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
import time
from typing import Union


class EZBC(ETP):
    """Franklin Templeton"""

    def url(self):
        return "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39639/SINGLCLASS/franklin-bitcoin-etf/EZBC"

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

        for name, content in self.files.items():
            try:
                t = BeautifulSoup(content, "html.parser")

                # Header
                ref_date_header = datetime.strptime(
                    t.find(class_="small").text.strip().split(" ")[-1],
                    "%m/%d/%Y"
                ).date().isoformat()
                market_price = float(t.find_all(class_="summary-item__value summary-item__value--basic ng-star-inserted")[-2].text.strip().strip("$"))

                # Fund Information
                tables = t.find(class_="col subgrid-desktop-small--layout-3 subgrid-desktop-small-gap--sm subgrid-lg-tablet--gap-sm subgrid-lg-tablet--layout-0 subgrid-mobile--layout-0 subgrid-mobile-gap--none subgrid-row-gap--none subgrid-tablet--layout-0 subgrid-tablet-gap--sm")
                ref_date_fund_information = datetime.strptime(
                    t.find_all(class_="info-item__as-of ng-star-inserted")[0].text.split(" ")[3],
                    "%m/%d/%Y"
                ).date().isoformat()
                market_cap = float(tables.find_all(class_="info-item__val info-item__val--default ng-star-inserted")[3].text.split(" ")[1].strip("$"))
                n_shares = int(tables.find_all(class_="info-item__val info-item__val--60-40 ng-star-inserted")[7].text.replace(",", "").strip())
                daily_share_volume = int(tables.find_all(class_="info-item__val info-item__val--60-40 ng-star-inserted")[8].text.replace(",", ""))
                n_coins = float(tables.find_all(class_="info-item__val info-item__val--default ng-star-inserted")[4].text.strip())

                self.extracted[name] = {
                    "file_name": name,
                    "ref_date_header": ref_date_header,
                    "market_price": market_price,
                    "ref_date_fund_information": ref_date_fund_information,
                    "market_cap": market_cap,
                    "daily_share_volume": daily_share_volume,
                    "n_shares": n_shares,
                    "n_coins": n_coins
                }

            except:
                # TODO: log failure
                pass

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        header = df[["file_name"] + list(df.columns[1:3])]
        header = header.rename({"ref_date_header": "ref_date"}, axis=1)
        table = "ezbc_header"
        keys = "ref_date"

        self._dump(header, table, keys, con)

        ##################
        fund_information = df[["file_name"] + list(df.columns[3:])]
        fund_information = fund_information.rename({"ref_date_fund_information": "ref_date"}, axis=1)
        table = "ezbc_fund_information"
        keys = "ref_date"

        self._dump(fund_information, table, keys, con)
