from datetime import datetime
import os
from pathlib import Path
from product.abc import ETP
import requests
from sqlite3 import Connection
from typing import Union


class BTC(ETP):
    """GrayScale"""

    def url(self):
        return "https://etfs.grayscale.com/btc"

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
        raise NotImplementedError

    def update_db(self, con: Connection) -> None:
        raise NotImplementedError
