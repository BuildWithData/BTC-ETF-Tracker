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
from typing import Union
from utils.constants import MONTH2NUMBER


class FBTC(ETP):
    """Fidelity"""

    def url(self):
        return "https://digital.fidelity.com/prgw/digital/research/quote/dashboard/summary?symbol=FBTC"

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

        file_timestamps = set([k.split(".")[0] for k in self.files.keys()])

        for ts in file_timestamps:

            fn_html = ".".join([ts, "html"])
            content = self.files[fn_html]
            t = BeautifulSoup(content, "html.parser")

            ref_date_html = t.find(class_="nre-quick-quote-left-third-row").find("span").text.split(" ")[-1]
            for name, number in MONTH2NUMBER.items():
                ref_date_html = ref_date_html.upper().replace(name, number)
            ref_date_html = datetime.strptime(ref_date_html, "%m-%d-%Y").date().isoformat()

            market_price = float(t.find(class_="nre-quick-quote-price-and-change-container ng-star-inserted").find(class_="nre-quick-quote-price").text.strip("$"))
            #daily_share_volume = int(t.find(class_="nre-quick-quote-right-col market-state-status ng-star-inserted").find_all(class_="col-left ng-star-inserted")[1].text.strip().replace(",", ""))

            fn_xls = ".".join([ts, "xls"])
            content = self.files[fn_xls]

            ref_date_xls = content.loc[1]["FIDELITY WISE ORIGIN BITCOIN FUND"]
            for name, number in MONTH2NUMBER.items():
                ref_date_xls = ref_date_xls.upper().replace(name, number)
            ref_date_xls = ref_date_xls.replace("-2", "-202")
            ref_date_xls = datetime.strptime(ref_date_xls, "%d-%m-%Y").date().isoformat()

            n_shares = content.loc[2]["FIDELITY WISE ORIGIN BITCOIN FUND"]
            n_coins = content.loc[6]["Unnamed: 12"]
            market_cap_coins = content.loc[6]["Unnamed: 14"]
            btc_ref_price = round(market_cap_coins / n_coins, 2)

            self.extracted[ts] = {
                "file_name_html": fn_html,
                "ref_date_html": ref_date_html,
                "market_cap": None,
                "daily_share_volume": None,
                "n_shares": n_shares,
                "market_price": market_price,
                "file_name_xls": fn_xls,
                "ref_date_xls": ref_date_xls,
                "n_coins": n_coins,
                "btc_ref_price": btc_ref_price
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        html = df[df.columns[:6]]
        html = html.rename({"file_name_html": "file_name", "ref_date_html": "ref_date"}, axis=1)
        table = "fbtc_html"
        keys = "ref_date"

        self._dump(html, table, keys, con)

        ##################
        xls = df[df.columns[6:]]
        xls = xls.rename({"file_name_xls": "file_name", "ref_date_xls": "ref_date"}, axis=1)
        table = "fbtc_xls"
        keys = "ref_date"

        self._dump(xls, table, keys, con)
