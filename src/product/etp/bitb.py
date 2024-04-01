from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from product.abc import ETP
from sqlite3 import Connection
from typing import Union
import json
import logging
import os
import pandas as pd
import requests


LOGGER = logging.getLogger(__name__)


class BITB(ETP):
    """BitWise"""

    def url(self):
        return "https://bitbetf.com/"

    def _file_extension(self):
        return "html"

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        response = requests.get(self.url())

        if response.ok is False:
            raise RuntimeError

        path = os.path.join(self.path(), self._file_name(timestamp))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(response.text)

    def extract(self):

        for name, content in self.files.items():
            try:
                t = BeautifulSoup(content, "html.parser")
                t = list(list(list(list(t.children)[-1].children)[-1].children)[-1].children)[0]
                t = json.loads(t)

                # Fund Details
                ref_date_fund_details = t["props"]["pageProps"]["fundData"]["data"]["holdings"]["asOfDate"]
                market_cap = t["props"]["pageProps"]["fundData"]["data"]["fundDetails"]["netAssets"] # TODO: net_assets_AUM
                daily_shares_volume_ref_date = t["props"]["pageProps"]["fundData"]["data"]["premiumDiscount"]["asOfDate"]
                daily_shares_volume = t["props"]["pageProps"]["fundData"]["data"]["premiumDiscount"]["volume"]
                n_shares = t["props"]["pageProps"]["fundData"]["data"]["fundDetails"]["sharesOutstanding"]

                # Premium / Discount
                ref_date_premium_discount = t["props"]["pageProps"]["fundData"]["data"]["navAndMarketPrice"]["asOfDate"]
                closing_price = t["props"]["pageProps"]["fundData"]["data"]["navAndMarketPrice"]["marketPrice"] # TODO: market_price

                # Fund Holdings
                ref_date_fund_holdings = t["props"]["pageProps"]["fundData"]["data"]["holdings"]["asOfDate"]
                btc_in_trust = t["props"]["pageProps"]["fundData"]["data"]["holdings"]["basket"][0]["shares"]

                # Bitcoin Holding Addresses
                ref_date_bitcoin_holding_addresses = t["props"]["pageProps"]["fundData"]["data"]["wallets"]["updatedAt"]
                addresses = t["props"]["pageProps"]["fundData"]["data"]["wallets"]["walletBalances"]
                n_coins = sum([e["balance"] for e in addresses])

                self.extracted[name] = {
                    "file_name": name,

                    "ref_date_fund_details": ref_date_fund_details,
                    "market_cap": market_cap,
                    "daily_shares_volume_ref_date": daily_shares_volume_ref_date,
                    "daily_shares_volume": daily_shares_volume,
                    "n_shares": n_shares,
                    "ref_date_premium_discount": ref_date_premium_discount,
                    "closing_price": closing_price,
                    "ref_date_fund_holdings": ref_date_fund_holdings,
                    "btc_in_trust": btc_in_trust,
                    "ref_date_bitcoin_holding_addresses": ref_date_bitcoin_holding_addresses,
                    "addresses": addresses,
                    "n_coins": n_coins,

                }

            except:
                # TODO: log fail
                pass

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values()).drop("addresses", axis=1)

        ##################
        fund_details = df[["file_name"] + list(df.columns[1:6])]
        fund_details = fund_details.rename({"ref_date_fund_details": "ref_date"}, axis=1)
        table = "bibt_fund_details"
        keys = ["ref_date", "daily_shares_volume_ref_date"]

        self._dump(fund_details, table, keys, con)

        ##################
        premium_discount = df[["file_name"] + list(df.columns[6:8])]
        premium_discount = premium_discount.rename({"ref_date_premium_discount": "ref_date"}, axis=1)
        table = "bibt_premium_discount"
        key = "ref_date"

        self._dump(premium_discount, table, key, con)

        ##################
        fund_holdings = df[["file_name"] + list(df.columns[8:10])]
        fund_holdings = fund_holdings.rename({"ref_date_fund_holdings": "ref_date"}, axis=1)
        table = "bibt_fund_holdings"
        key = "ref_date"

        self._dump(fund_holdings, table, key, con)

        con.commit()
