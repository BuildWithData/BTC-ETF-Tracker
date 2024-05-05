import argparse
import logging
import pandas as pd
import sqlite3
from utils.config import CONSUMPTION_US_SCHEMA_PATH
from utils.constants import TICKERS


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

parser = argparse.ArgumentParser(description="update table inflows_btc")
parser.add_argument("-d", "--date", help="target date", required=False)
parser.add_argument("-f", "--force", help="force loading even if data have been already written for yyyy-mm-dd", action="store_const", const=True)

conn = sqlite3.connect(CONSUMPTION_US_SCHEMA_PATH)
c = conn.cursor()

###################
# INPUTS
args = parser.parse_args()
ref_date = args.date
force = args.force

QUERY = "select * from holdings_btc_bfill"

##################
# READ
extracted = pd.DataFrame(
    c.execute(QUERY),
    columns=["ref_date", "week", "day"] + TICKERS + ["TOTAL"],
)

##################
# PARSE
out = (extracted.iloc[:, 3:] - extracted.iloc[:, 3:].shift(1)).round(2)
out["ref_date"] = extracted.ref_date
out["week"] = extracted.week
out["day"] = extracted.day
out = out[out["ref_date"] > "2024-02-23"]
out = out[["ref_date", "week", "day"] + TICKERS + ["TOTAL"]]

if ref_date is not None:
    out = out[out["ref_date"] == ref_date]

##################
# LOAD
if force is True:

    DELETE_QUERY = "DELETE FROM inflows_btc_bfill "

    if ref_date is not None:
        DELETE_QUERY += f"WHERE ref_date = '{ref_date}'"

    c.execute(DELETE_QUERY)


for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO inflows_btc_bfill VALUES ("
    INSERT_QUERY += f"'{row[1]}', '{row[2]}', '{row[3]}'"

    for v in row[4:]:
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
