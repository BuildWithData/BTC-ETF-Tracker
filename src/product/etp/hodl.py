from datetime import datetime
from pathlib import Path
from product.abc import ETP
from sqlite3 import Connection
from typing import Union
import json
import os
import pandas as pd
import requests


class HODL(ETP):
    """VanEck"""

    def url(self):
        return {
            "main": "https://www.vaneck.com/Main/NavInformationBlock/GetContent/?blockid=252190&pageid=243755&ticker=HODL&reactlang=en&reactctr=us&epieditmode=false&latest=false",
            "volume": {
                "url": "https://www.vaneck.com/Main/FundListingUs/GetFundData",
                "data": {'filterJson': '{"InvType":"etf","AssetClass":["c","nr","se","t","cb","ei","ib","mb","fr","c-da","c-g","c-ra","ma"],"Funds":["emf","esf","grf","iigf","mwmf","emlf","embf","ccif"],"ShareClass":["a","c","i","y","z"],"TableType":"price-returns","SortCol":"ticker","IsAsc":true,"FilterFunds":["HODL"],"CurrentPageId":"5517"}'}
            }
        }

    def _file_extension(self):
        return "json"

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        response_main = requests.get(self.url()["main"])
        volume = self.url()["volume"]
        response_volume = requests.post(volume["url"], data=volume["data"])

        if response_main.ok is False:
            raise RuntimeError

        if response_volume.ok is False:
            raise RuntimeError

        path = os.path.join(self.path(), self._file_name(timestamp))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            out = {"main": response_main.json(), "volume": response_volume.json()}
            json.dump(out, f)

    def extract(self):

        for name, content in self.files.items():

            ref_date = datetime.strptime(content["main"]["data"]["AsOfDate"], "%m/%d/%Y").date().isoformat()
            market_cap = float(content["main"]["data"]["Navs"][0]["Value"].replace(",", ""))
            n_shares = int(content["main"]["data"]["Navs"][-3]["Value"].replace(",", ""))
            n_coins = float(content["main"]["data"]["Navs"][-2]["Value"].replace(",", ""))
            market_price = float(content["volume"]["Result"]["FundSet"][0]["RowData"][3]["SubItems"][1]["DisplayValue"].strip("$"))

            self.extracted[name] = {
               "file_name": name,
               "ref_date": ref_date,
               "market_cap": market_cap,
               "n_shares": n_shares,
               "n_coins": n_coins,
               "market_price": market_price,
               "daily_volume_traded": None
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        table = "hodl"
        keys = "ref_date"

        self._dump(df, table, keys, con)
