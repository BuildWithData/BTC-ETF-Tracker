import argparse
from datetime import date
from functools import reduce
import logging
import pandas as pd
import sqlite3
import sys
from utils.config import CONSUMPTION_HK_SCHEMA_PATH
from utils.config import RAW_SCHEMA_PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

parser = argparse.ArgumentParser(description="update table holdings_btc")
parser.add_argument("-d", "--date", help="target date", required=False)
parser.add_argument("-f", "--force", help="force loading even if data have been already written for yyyy-mm-dd", action="store_const", const=True)

conn_raw = sqlite3.connect(RAW_SCHEMA_PATH)
c_raw = conn_raw.cursor()

conn_con = sqlite3.connect(CONSUMPTION_HK_SCHEMA_PATH)
c_con = conn_con.cursor()

QUERIES = {
    "AB9042": "select ref_date, n_coins as AB9042 from ab9042_xlsx",
    "AE9046": "select ref_date, n_coins as AE9046 from ae9046_xlsx",
    "BB9008": "select ref_date, n_coins as BB9008 from bb9008_xlsx",
    "BE9009": "select ref_date, n_coins as BE9009 from be9009_xlsx",
    "HB9439": """
                select
                    h.ref_date,
                    round(h.fund_size * h.weight_virtual_asset / (a.market_price + b.market_price) * 2, 2) as n_coins

                from hb9439 as h

                join ab9042_xlsx as a
                    on h.ref_date = a.ref_date

                join bb9008_xlsx as b
                    on h.ref_date = b.ref_date

    """,
    "HE9179": """
                select
                    h.ref_date,
                    round(h.fund_size * h.weight_virtual_asset / (a.market_price + b.market_price) * 2, 2) as n_coins

                from he9179 as h

                join ae9046_xlsx as a
                    on h.ref_date = a.ref_date

                join be9009_xlsx as b
                    on h.ref_date = b.ref_date
    """

}

extracted = {}

################
# INPUTS
args = parser.parse_args()
ref_date = args.date
force = args.force

################
# READ
for ticker, query in QUERIES.items():

    if ref_date is not None:
        query += f" where ref_date = '{ref_date}'"

    data = list(c_raw.execute(query))

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
if force is True:

    DELETE_QUERY = "DELETE FROM holdings "

    if ref_date is not None:
        DELETE_QUERY += f"WHERE ref_date = '{ref_date}'"

    c_con.execute(DELETE_QUERY)


for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO holdings VALUES ("
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
