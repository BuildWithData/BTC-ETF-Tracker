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


class ARKB(ETP):
    """ArkInvest"""

    def url(self, type: str = None):
        out = {
            "html": "https://ark-funds.com/funds/arkb/",
            "csv": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv"
        }
        if type is not None:
            out = out[type]
        return out

    def _file_extension(self):
        raise NotImplementedError

    def _csv_original_file_name(self):
        return self.url("csv").split("/")[-1]

    def _file_name(self, scrape_timestamp: datetime, type: str):
        out = f"{self.ticker}_{scrape_timestamp.isoformat(timespec='minutes')}"
        out += "." + type
        return out

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        options = Options()

        #options.add_argument('--headless')

        options.add_experimental_option("prefs", {
                "download.default_directory": self.path(),
        })
        driver = webdriver.Chrome(options)
        driver.get(self.url("html"))

        driver.find_element(By.ID, "continue_us").click()
        driver.find_element(By.ID, "agree_button")
        driver.find_element(By.ID, "agree_button").click()
        driver.find_element(By.ID, "hs-eu-confirmation-button").click()

        # TODO: error handling

        path = os.path.join(self.path(), self._file_name(timestamp, "html"))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(driver.page_source)

        driver.execute_script("window.scrollTo(0, 3500)")
        sleep(5) # must wait to get to the page section and then can click, otherwise it fails
        driver.find_elements(By.CLASS_NAME, 'b-table__link')[-1].click()

        actual = Path(os.path.join(self.path(), self._csv_original_file_name()))
        sleep(2)
        if actual.exists() is False:
            raise RuntimeError(f"File {actual} does not exist")
        new = os.path.join(self.path(), self._file_name(timestamp, "csv"))
        actual.rename(new)

    def extract(self):

        file_timestamps = set([k.split(".")[0] for k in self.files.keys()])

        for ts in file_timestamps:

            fn_html = ".".join([ts, "html"])
            content = self.files[fn_html]
            t = BeautifulSoup(content, "html.parser")

            # NAV Historical Change
            market_price = float(t.find(class_="col-xl-4 col-lg-4").find_all("span")[3].text.strip().strip("$"))
            daily_share_volume = int(t.find(class_="col-xl-4 col-lg-4").find_all("span")[5].text.split(" ")[0].strip().replace(",", ""))
            ref_date_html = datetime.strptime(
                t.find(class_="b-promo-funds__item-date").text.split(" ")[2],
                "%m/%d/%Y"
            ).date().isoformat() # TODO: wrong

            fn_csv = ".".join([ts, "csv"])
            content = self.files[fn_csv]
            ref_date_csv = datetime.strptime(content.loc[0, "date"], "%m/%d/%Y").date().isoformat()
            n_coins = float(content.loc[0, "shares"].replace(",", ""))
            # TODO: check with market cap in html... should me similar but not equal -> then add it
            btc_ref_price = round(float(content.loc[0, "market value ($)"].strip("$").replace(",", "")) / n_coins, 2)

            self.extracted[ts] = {
                "file_name_html": fn_html,
                "ref_date_html": ref_date_html,
                "market_cap": None,
                "daily_share_volume": daily_share_volume,
                "n_shares": None,
                "market_price": market_price,
                "file_name_csv": fn_csv,
                "ref_date_csv": ref_date_csv,
                "n_coins": n_coins,
                "btc_ref_price": btc_ref_price
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        html = df[df.columns[:6]]
        html = html.rename({"file_name_html": "file_name", "ref_date_html": "ref_date"}, axis=1)
        table = "arkb_html"
        keys = "ref_date"

        self._dump(html, table, keys, con)

        ##################
        csv = df[df.columns[6:]]
        csv = csv.rename({"file_name_csv": "file_name", "ref_date_csv": "ref_date"}, axis=1)
        table = "arkb_csv"
        keys = "ref_date"

        self._dump(csv, table, keys, con)
