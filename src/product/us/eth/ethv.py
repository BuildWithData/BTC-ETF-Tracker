from datetime import datetime
import json
import os
from pathlib import Path
from product.abc import ETP
import requests
from sqlite3 import Connection
from typing import Union


class ETHV(ETP):
    """VanEck"""

    def url(self):
        return {
            "main": "https://www.vaneck.com/Main/NavInformationBlock/GetContent/?blockid=252190&pageid=243755&ticker=ETHV&reactlang=en&reactctr=us&epieditmode=false&latest=fa",
            "volume": {
                "url": "https://www.vaneck.com/Main/FundListingUs/GetFundData",
                "data": {'filterJson': '{"InvType":"etf","AssetClass":["c","nr","se","t","cb","ei","ib","mb","fr","c-da","c-g","c-ra","ma"],"Funds":["emf","esf","grf","iigf","mwmf","emlf","embf","ccif"],"ShareClass":["a","c","i","y","z"],"TableType":"price-returns","SortCol":"ticker","IsAsc":true,"FilterFunds":["ETHV"],"CurrentPageId":"5517"}'}
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
        raise NotImplementedError

    def update_db(self, con: Connection) -> None:
        raise NotImplementedError
