import argparse
from datetime import date
import logging
import pandas as pd
import sqlite3
from utils.config import CONSUMPTION_SCHEMA_PATH
from utils.constants import TICKERS


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

parser = argparse.ArgumentParser(description="update table inflows_btc")
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "-d",
    "--date",
    help="target date",
    required=False,
    type=date.fromisoformat
)
group.add_argument(
    "-fd",
    "--from-date",
    help="from target date",
    required=False,
    type=date.fromisoformat
)

conn = sqlite3.connect(CONSUMPTION_SCHEMA_PATH)
c = conn.cursor()

###################
# INPUTS
args = parser.parse_args()
ref_date = args.date
from_ref_date = args.from_date

QUERY = "select * from holdings_btc"

##################
# READ
extracted = pd.DataFrame(
    c.execute(QUERY),
    columns=["ref_date"] + TICKERS
)

##################
# PARSE
out = (extracted.iloc[:, 1:] - extracted.iloc[:, 1:].shift(1)).round(2)
out["ref_date"] = extracted.ref_date.apply(date.fromisoformat)
out = out[out["ref_date"] > date.fromisoformat("2024-02-23")]
out["TOTAL"] = out.iloc[:, :-1].sum(axis=1).round(2)
out = out[["ref_date"] + TICKERS + ["TOTAL"]]

if ref_date is not None:
    out = out[out["ref_date"] == ref_date]

elif from_ref_date is not None:
    out = out[out["ref_date"] >= from_ref_date]


##################
# LOAD
for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO inflows_btc VALUES ("
    INSERT_QUERY += f"'{row[1]}'"

    for v in row[2:]:
        INSERT_QUERY += ","
        if pd.isna(v):
            v = 'Null'
        INSERT_QUERY += f"{v}"

    INSERT_QUERY += ")"

    try:
        c.execute(INSERT_QUERY)
        LOGGER.info(f"Written data for {row[1]}")

    except sqlite3.IntegrityError:
        LOGGER.error(f"Data for {row[1]} already found in table")

conn.commit()
conn.close()
