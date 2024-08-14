from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
import requests
from sqlite3 import Connection
from typing import Union
import json


class CETH(ETP):
    """21Shares"""

    def url(self):
        return "https://www.21shares.com/en-us/product/ceth"

    def _file_extension(self, type: str):
        out = {
            "html": "html",
            "api": "json"
        }
        return out[type]

    def _file_name(self, scrape_timestamp: datetime, type: str):
        out = f"{self.ticker}_{scrape_timestamp.isoformat(timespec='minutes')}"
        out += "." + self._file_extension(type)
        return out

    def scrape(self) -> Union[Exception, None]:

        timestamp = datetime.today()
        response = requests.get(self.url())

        if response.ok is False:
            raise RuntimeError

        path = os.path.join(self.path(), self._file_name(timestamp, "html"))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            f.write(response.text)


        params = {
            'name': 'CETH',
        }

        response = requests.get(
            'https://xvmd-hnpa-7dsw.n7c.xano.io/api:hDe_3sVO/get_product_details_constituents',
            params=params,
        )

        if response.ok is False:
            raise RuntimeError

        path = os.path.join(self.path(), self._file_name(timestamp, "api"))
        path = Path(path)

        self._create_path(path)
        with open(path, "w") as f:
            out = response.json()
            json.dump(out, f)

    def extract(self):
        raise NotImplementedError

    def update_db(self, con: Connection) -> None:
        raise NotImplementedError
