from abc import ABC
from abc import abstractmethod
from datetime import date
from datetime import datetime
from pathlib import Path
from product.utils import get_insert_query
from product.utils import get_last_row_from_table
from product.utils import get_new_rows
from product.utils import get_rows_to_update
from product.utils import get_update_query
from typing import Any
from typing import List
from utils.config import DATA_BASE_PATH
import json
import logging
import os
import pandas as pd


LOGGER = logging.getLogger(__name__)


class Products(ABC):

    _scraping_failed = None
    type = None

    def __init__(self, date_: str):
        self.date = date.fromisoformat(date_)
        self.extracted = {}
        self.files = {}
        self.ticker = self.__repr__()

    def __repr__(self):
        return self.__class__.__name__

    @classmethod
    def __str__(cls):
        return cls.__name__

    @property
    @abstractmethod
    def url(self):
        pass

    @property
    @abstractmethod
    def _file_extension(self):
        pass

    def _file_extension_from_path(self, path: Path):
        return path.as_posix().split(".")[-1]

    def _file_name(self, scrape_timestamp: datetime):
        out = f"{self.ticker}_{scrape_timestamp.isoformat(timespec='minutes')}"
        out += "." + self._file_extension()
        return out

    @abstractmethod
    def scrape(self, dry_run: bool) -> Any:
        raise NotImplementedError("Logic to scrape data and dump it")

    ################################

    def _create_path(self, path: Path) -> None:
        # create ticker folder
        if path.parent.parent.parent.parent.exists() is False:
            path.parent.parent.parent.parent.mkdir()

        # create year folder
        if path.parent.parent.parent.exists() is False:
            path.parent.parent.parent.mkdir()

        # create month folder
        if path.parent.parent.exists() is False:
            path.parent.parent.mkdir()

        # create day folder
        if path.parent.exists() is False:
            path.parent.mkdir()

    def path(self, timestamp=None) -> str:

        date_ = self.date
        file_name = ""

        if timestamp is not None:
            date_ = timestamp

        partition = f"raw/{self.ticker}/{date_.year}/{date_.month}/{date_.day}"

        if timestamp is not None:
            file_name = f"{self.ticker}_{date_.isoformat(timespec='minutes')}.{self.file_extension}"

        return os.path.join(DATA_BASE_PATH, partition, file_name)

    def ls_files(self) -> List:
        path = Path(self.path())
        return list(path.iterdir())

    def read(self) -> None:

        for path in self.ls_files():
            key = path.name
            fe = self._file_extension_from_path(path)

            if fe == "html":
                with open(path, "r") as f:
                    data = f.read()

            elif fe == "csv":
                data = pd.read_csv(path)

            elif fe in ("xls", "xlsx"):
                data = pd.read_excel(path)

            elif fe == "json":
                with open(path, "r") as f:
                    data = json.load(f)

            else:
                raise ValueError(f"not implemented file extension: {self._file_extension()}")

            self.files[key] = data

    ################################

    def _dump(self, data, table_name, keys, con):

        db = get_last_row_from_table(table_name, keys, data.columns, con)
        to_update = get_rows_to_update(data, db, keys)

        if to_update is not None:
            LOGGER.info(f"Updating value in {table_name}")
            query = get_update_query(to_update, table_name, keys)
            con.execute(query)

        new = get_new_rows(data, db, keys)

        if new is not None:
            LOGGER.info(f"Writing new values in {table_name}")
            query = get_insert_query(new, table_name)
            con.execute(query)
        con.commit()

    @abstractmethod
    def extract(self):
        raise NotImplementedError("Logic")

    @abstractmethod
    def update_db(self) -> None:
        raise NotImplementedError


class ETP(Products):
    """ Bitcoin only """
    type = "ETP"


class ETF(Products):
    """ Bitcoin + Altcoin """
    type = "ETF"
