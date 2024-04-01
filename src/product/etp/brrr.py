from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from product.abc import ETP
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlite3 import Connection
from typing import Union
import os
import pandas as pd


class BRRR(ETP):
    """Valkyrie"""

    def url(self):
        return "https://valkyrieinvest.com/brrr/"

    def _file_extension(self):
        return "html"

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        options = Options()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options)
        driver.get(self.url())

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

                # Fund Summary
                fund_summary_section = t.find(class_="mcb-wrap-inner mcb-wrap-inner-d7b93a111 mfn-module-wrapper mfn-wrapper-for-wraps")
                ref_date_fund_summary = datetime.strptime(
                    fund_summary_section.find("p").text.split(" ")[-1],
                    "%m/%d/%Y"
                ).date().isoformat()
                market_price = float(fund_summary_section.find_all("dd")[16].text)
                btc_ref_price = float(fund_summary_section.find_all("dd")[20].text.replace(",", ""))

                # Holdings
                holdings_section = t.find_all(class_="mcb-wrap-inner mcb-wrap-inner-5a0dc249f mfn-module-wrapper mfn-wrapper-for-wraps")[0].find("table")

                ref_date_holdings = datetime.strptime(
                    t.find_all(class_="mcb-wrap-inner mcb-wrap-inner-5a0dc249f mfn-module-wrapper mfn-wrapper-for-wraps")[0].find(class_="column mcb-column mcb-item-118575426 one laptop-one tablet-one mobile-one column_column").find("p").text.split(" ")[-1],
                    "%m/%d/%Y"
                ).date().isoformat()
                market_cap = float(holdings_section.find_all("td")[7].text.replace(",", ""))  # TODO: market value but this is NAV market cap
                n_coins = float(holdings_section.find_all("td")[6].text.replace(",", ""))

                self.extracted[name] = {
                    "file_name": name,
                    "ref_date_fund_summary": ref_date_fund_summary,
                    "daily_traded_volume": None,
                    "n_shares": None,
                    "market_price": market_price,
                    "btc_ref_price": btc_ref_price,
                    "ref_date_holdings": ref_date_holdings,
                    "market_cap": market_cap,
                    "n_coins": n_coins
                }

            except:
                # TODO: log fail
                pass

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        fund_summary = df[["file_name"] + list(df.columns[1:6])]
        fund_summary = fund_summary.rename({"ref_date_fund_summary": "ref_date"}, axis=1)
        table = "brrr_fund_summary"
        keys = "ref_date"

        self._dump(fund_summary, table, keys, con)

        ##################
        holdings = df[["file_name"] + list(df.columns[6:])]
        holdings = holdings.rename({"ref_date_holdings": "ref_date"}, axis=1)
        table = "brrr_holdings"
        keys = "ref_date"

        self._dump(holdings, table, keys, con)
