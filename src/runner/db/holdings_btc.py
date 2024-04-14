import argparse
from datetime import date
from functools import reduce
import logging
import numpy as np
import pandas as pd
import sqlite3
import sys
from utils.config import CONSUMPTION_SCHEMA_PATH
from utils.config import RAW_SCHEMA_PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

parser = argparse.ArgumentParser(description="update table holdings_btc")
parser.add_argument("-d", "--date", help="target date", required=False)

conn_raw = sqlite3.connect(RAW_SCHEMA_PATH)
c_raw = conn_raw.cursor()

conn_con = sqlite3.connect(CONSUMPTION_SCHEMA_PATH)
c_con = conn_con.cursor()

QUERIES = {
    "ARKB": "select ref_date, n_coins as ARKB from arkb_csv",
    "BIBT": "select ref_date, btc_in_trust as BIBT from bibt_fund_holdings",
    "BRRR": "select ref_date, n_coins as BRRR from brrr_holdings",
    "BTCO": "select ref_date, n_coins as BTCO from btco",
    "EZBC": "select ref_date, n_coins as EZBC from ezbc_fund_information",
    "FBTC": "select ref_date, n_coins as FBTC from fbtc_xls",
    "GBTC": "select ref_date, n_coins as GBTC from gbtc_key_fund_information",
    "HODL": "select ref_date, n_coins as HODL from hodl",
    "IBIT": "select ref_date, n_coins as IBIT from ibit"
}

extracted = {}

################
# INPUTS
args = parser.parse_args()
ref_date = args.date

################
# READ
for ticker, query in QUERIES.items():

    if ref_date is not None:
        query += f" where ref_date = '{ref_date}'"

    data = list(c_raw.execute(query))
    if ticker == "BTCO":
        # TODO: replace this with logic to calculate n_coins = MarketCap / btc_price
        data = [(t[0], np.NaN) for t in data]

    df = pd.DataFrame(data, columns=["ref_date", ticker])
    df[ticker] = df[ticker].round(2)
    extracted[ticker] = df

################
# PARSE
out = reduce(lambda x, y: x.merge(y, on="ref_date", how="outer"), extracted.values())
cols_out = ["ref_date", "week", "day"] + list(out.columns[1:].values)
out["week"] = out.ref_date.apply(lambda s: date.fromisoformat(s).isocalendar().week)
out["day"] = out.ref_date.apply(lambda s: date.fromisoformat(s).strftime("%a"))
out = out[cols_out]

if out.empty:
    LOGGER.warning(f"Found no data for {ref_date}")
    sys.exit(1)

################
# LOAD
for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO holdings_btc VALUES ("
    INSERT_QUERY += f"'{row[1]}', '{row[2]}', '{row[3]}'"

    for v in row[4:]:
        INSERT_QUERY += ","
        if pd.isna(v):
            v = 'Null'
        INSERT_QUERY += f"{v}"

    INSERT_QUERY += ")"

    try:
        c_con.execute(INSERT_QUERY)
        LOGGER.info(f"Written data for {row[1]}")

    except sqlite3.IntegrityError:
        LOGGER.error(f"Data for {row[1]} already found in table")


conn_con.commit()
conn_con.close()
conn_raw.close()
