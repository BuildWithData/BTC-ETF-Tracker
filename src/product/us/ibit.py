from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime
import os
import pandas as pd
from pathlib import Path
from product.abc import ETP
import requests
from sqlite3 import Connection
from typing import Union


class IBIT(ETP):
    """BlackRock"""

    def url(self):
        return "https://www.blackrock.com/us/financial-professionals/products/333011/ishares-bitcoin-trust"

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

        first_dom_change_date = date.fromisoformat("2024-05-22")

        for name, content in self.files.items():
            t = BeautifulSoup(content, "html.parser")
            key_facts_table = t.find(class_="product-data-list data-points-en_US")

            if self.date < first_dom_change_date:
                portfolio_characteristics = t.find(class_="float-left in-left col-levelAmount")
            else:
                portfolio_characteristics = t.find_all(class_="product-data-list data-points-en_US")[-1]

            ref_date = datetime.strptime(
                key_facts_table.find(class_="as-of-date").text.strip("\n").strip("as of").replace(",", ""),
                "%b %d %Y"
            ).date().isoformat()
            market_cap = int(key_facts_table.find(class_="data").text.strip("\n").strip("$").replace(",", ""))

            if self.date < first_dom_change_date:
                class_ = "float-left in-left col-consolidatedVolume"
            else:
                class_ = "product-data-item col-consolidatedVolume"
            daily_traded_volume = int(float(key_facts_table.find(class_=class_).find(class_="data").text.strip("\n").replace(",", "")))

            if self.date < first_dom_change_date:
                class_ = "float-left in-right col-sharesOutstanding"
            else:
                class_ = "product-data-item col-sharesOutstanding"
            n_shares = int(key_facts_table.find(class_=class_).find(class_="data").text.strip("\n").replace(",", ""))

            if self.date < first_dom_change_date:
                class_ = "float-left in-right col-closingPrice"
            else:
                class_ = "product-data-item col-closingPrice"
            closing_price = float(key_facts_table.find(class_=class_).find(class_="data").text.strip("\n"))

            btc_ref_price = float(portfolio_characteristics.find(class_="data").text.strip("\n").strip("USD ").replace(",", ""))
            n_coins = int(market_cap / btc_ref_price)

            self.extracted[name] = {
                "file_name": name,

                "ref_date": ref_date,
                "market_cap": market_cap,
                "daily_traded_volume": daily_traded_volume,
                "n_shares": n_shares,
                "closing_price": closing_price,
                "btc_ref_price": btc_ref_price,
                "n_coins": n_coins,
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        table = "ibit"
        keys = "ref_date"

        self._dump(df, table, keys, con)
