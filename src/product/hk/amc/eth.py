from datetime import datetime
import os
from numpy import NaN
import pandas as pd
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from sqlite3 import Connection
from time import sleep


class AE9046(ETP):
    """China AMC"""

    def url(self):
        return "https://www.chinaamc.com.hk/product/chinaamc-ether-etf/"

    def _file_extension(self):
        return "html"

    def _xlsx_original_file_name(self):
        return "HKVAETH_EN.xlsx"

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

        sleep(3)
        holding_section = [s for s in driver.find_elements(By.TAG_NAME, 'section') if "holding details" in s.text.lower()][1]
        holding_section.find_element(By.TAG_NAME, "a").click()
        actual = Path(os.path.join(self.path(), self._xlsx_original_file_name()))

        sleep(2)
        if actual.exists() is False:
            raise RuntimeError(f"File {actual} does not exist")
        new = os.path.join(self.path(), self._file_name(timestamp, "xlsx"))
        actual.rename(new)

    def extract(self):

        file_timestamps = set([k.split(".")[0] for k in self.files.keys()])

        for ts in file_timestamps:

            fn_xlsx = ".".join([ts, "xlsx"])
            df = self.files[fn_xlsx]

            ref_date_xlsx = df.iloc[0, 2].replace("/", "-")
            total_nav = df.iloc[1, 2]
            cash = df.iloc[2, 2]
            # sometimes cash in not reported, eg 2024-05-20
            if cash is NaN:
                cash = 0
            market_price = df.iloc[5, 6]

            n_coins = round((total_nav - cash) / market_price, 2)

            self.extracted[ts] = {
                "file_name_xlsx": fn_xlsx,
                "ref_date_xlsx": ref_date_xlsx,
                "total_nav": total_nav,
                "cash": cash,
                "market_price": market_price,
                "n_coins": n_coins
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        xlsx = df.rename({"file_name_xlsx": "file_name", "ref_date_xlsx": "ref_date"}, axis=1)
        table = "ae9046_xlsx"
        keys = "ref_date"

        self._dump(xlsx, table, keys, con)
