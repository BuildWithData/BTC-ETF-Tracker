from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime
from pathlib import Path
from product.abc import ETP
from sqlite3 import Connection
from typing import Union
import os
import pandas as pd
import requests


class GBTC(ETP):
    """GrayScale"""

    def url(self):
        return "https://etfs.grayscale.com/gbtc"

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
            t = BeautifulSoup(content, "html.parser")

            # Key Fund Information
            key_fund_information = t.find_all(class_="Tables_Tables__container_table__Tw2_H")[-1]

            if self.date < date.fromisoformat("2024-03-04"):
                ref_date = key_fund_information.find(class_="Text_Text__xt7Jy Text_Text_body-small__79DVN").text.split(" ")[-1]

            else:
                ref_date = key_fund_information.find(class_="DownloadWithDate_Root__xBAtE DownloadWithDate_Root_white__Zr0dU").text.split(" ")[-1]

            ref_date_key_fund_information = datetime.strptime(ref_date, "%m/%d/%Y").date().isoformat()
            # TODO: not market cap but AUM
            market_cap = float(key_fund_information.find_all(class_="TableItem_TableItem__b9eHf")[0].text.split("$")[-1].replace(",", ""))
            n_shares = int(key_fund_information.find_all(class_="TableItem_TableItem__b9eHf")[2].text.split("g")[-1].replace(",", ""))
            n_coins = float(key_fund_information.find_all(class_="TableItem_TableItem__b9eHf")[6].text.split("t")[-1].replace(",", ""))

            # Daily Performance
            ref_date_daily_performance = t.find(class_="DownloadWithDate_Root__xBAtE TablesContentWithHeader_Table__headDownload__L_sjH DownloadWithDate_Root_black__nxdup").text.split(" ")[-1]
            ref_date_daily_performance = datetime.strptime(ref_date_daily_performance, "%m/%d/%Y").date().isoformat()
            performance = t.find_all(class_="Table_Table__list__vmk_3")[2]

            market_price = float(performance.find_all(class_="Text_Text__xt7Jy Text_Text_body__t0eXi TableItem_TableItem__value___2Fjx")[6].text.strip("$"))
            daily_share_volume_traded = int(performance.find_all(class_="Text_Text__xt7Jy Text_Text_body__t0eXi TableItem_TableItem__value___2Fjx")[3].text.replace(",", ""))

            self.extracted[name] = {
               "file_name": name,
               "ref_date_key_fund_information": ref_date_key_fund_information,
               "market_cap": market_cap,
               "n_shares": n_shares,
               "n_coins": n_coins,
               "ref_date_daily_performance": ref_date_daily_performance,
               "market_price": market_price,
               "daily_share_volume_traded": daily_share_volume_traded
            }

    def update_db(self, con: Connection) -> None:

        df = pd.DataFrame(self.extracted.values())

        ##################
        key_fund_information = df[["file_name"] + list(df.columns[1:5])]
        key_fund_information = key_fund_information.rename({"ref_date_key_fund_information": "ref_date"}, axis=1)
        table = "gbtc_key_fund_information"
        keys = "ref_date"

        self._dump(key_fund_information, table, keys, con)

        ##################
        daily_performance = df[["file_name"] + list(df.columns[5:])]
        daily_performance = daily_performance.rename({"ref_date_daily_performance": "ref_date"}, axis=1)
        table = "gbtc_daily_performance"
        keys = "ref_date"

        self._dump(daily_performance, table, keys, con)
