from datetime import date
import logging
from numpy import NaN
import pandas as pd
import sqlite3
from utils.config import CONSUMPTION_SCHEMA_PATH
from utils.constants import TICKERS
import warnings


warnings.simplefilter(action='ignore', category=FutureWarning) # fillna

LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(CONSUMPTION_SCHEMA_PATH)
c = conn.cursor()

######################
# READ
QUERY = "select * from inflows_btc_bfill"
inflows = pd.DataFrame(c.execute(QUERY), columns=["ref_date", "week", "day"] + TICKERS + ["TOTAL"])

QUERY = "select * from holdings_btc_bfill"
holdings = pd.DataFrame(c.execute(QUERY), columns=["ref_date", "week", "day"] + TICKERS + ["TOTAL"])

PATH_BTMX = '/Users/bwd/Code-From-Videos/btc_etf_holdings/data/raw/external/bitmex/btc_flow.csv'
btmx = pd.read_csv(PATH_BTMX)

######################
# PARSE
inflows = inflows.drop("week", axis=1)
inflows = inflows.drop("day", axis=1)
holdings = holdings.drop("week", axis=1)
holdings = holdings.drop("day", axis=1)

btmx_comulative_inflows_at_02_23 = btmx[btmx.ref_date <= "2024-02-23"][TICKERS].sum()
btmx_holdings_day0 = btmx.loc[1]
btmx_holdings_at_02_23 = btmx_comulative_inflows_at_02_23 + btmx_holdings_day0

holdings_at_02_23 = holdings[holdings["ref_date"] == "2024-02-23"].sum()

delta = (holdings_at_02_23 - btmx_holdings_at_02_23).round(2) # TODO: round not working
delta = pd.DataFrame(delta).T
delta = delta.drop("BTCW", axis=1) # TODO: remove when BTCW data available
delta["ref_date"] = "2024-02-23"

inflows = inflows[inflows["ref_date"] >= "2024-02-26"]

btmx = btmx[btmx["ref_date"] < "2024-02-23"]
btmx = btmx[btmx["ref_date"] > "2024-01-10"]
btmx = btmx[btmx.apply(lambda r: not (pd.isna(r.ARKB) and r.ref_date not in ("2024-01-15", "2024-02-19")), axis=1)]
btmx = btmx.drop("BTCW", axis=1)

out = pd.concat([btmx, delta, inflows]).reset_index(drop=True)
out = out.drop("BTCO", axis=1) # TODO: remove when BTCO data available
out = out.drop("TOTAL", axis=1)
out["TOTAL"] = out.iloc[:, 1:].sum(axis=1).round(2)
out["week"] = out.ref_date.apply(lambda s: date.fromisoformat(s).isocalendar().week)
out["day"] = out.ref_date.apply(lambda s: date.fromisoformat(s).strftime("%a"))
# btmx missing data
out.iloc[2, -1] = NaN
out.iloc[27, -1] = NaN
out = out[["ref_date", "week", "day"] + [t for t in TICKERS if t != "BTCO"] + ["TOTAL"]]

##################
# LOAD

CLEAN_TABLE_QUERY = "DELETE FROM inflows_btc_bxfill"
c.execute(CLEAN_TABLE_QUERY)

for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO inflows_btc_bxfill VALUES ("
    INSERT_QUERY += f"'{row[1]}', '{row[2]}', '{row[3]}'"

    for v in row[4:]:
        INSERT_QUERY += ","
        if pd.isna(v):
            v = 'Null'
        INSERT_QUERY += f"{v}"

    INSERT_QUERY += ")"

    c.execute(INSERT_QUERY)


LOGGER.info(f"Updated data at {row[1]}")

conn.commit()
conn.close()
