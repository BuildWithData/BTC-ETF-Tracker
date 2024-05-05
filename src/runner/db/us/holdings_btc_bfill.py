import argparse
import logging
import pandas as pd
import sqlite3
import sys
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

parser = argparse.ArgumentParser(description="update table holdings_btc_bfill")
parser.add_argument("-d", "--date", help="target date", required=False)
parser.add_argument("-f", "--force", help="force loading even if data have been already written for yyyy-mm-dd", action="store_const", const=True)

conn = sqlite3.connect(CONSUMPTION_SCHEMA_PATH)
c = conn.cursor()

################
# INPUTS
args = parser.parse_args()
ref_date = args.date
force = args.force

################
# READ
query = "select * from holdings_btc_us"

data = list(c.execute(query))
# TODO: this should be dynamic
df = pd.DataFrame(data, columns=["ref_date", "week", "day"] + TICKERS)

################
# PARSE
out = df.fillna(method="ffill")
out["TOTAL"] = out.iloc[:, 3:].sum(axis=1)

if ref_date is not None:
    out = out[out.ref_date == ref_date]

if out.empty:
    LOGGER.warning(f"Found no data for {ref_date}")
    sys.exit(1)

################
# LOAD
if force is True:

    DELETE_QUERY = "DELETE FROM holdings_btc_bfill_us "

    if ref_date is not None:
        DELETE_QUERY += f"WHERE ref_date = '{ref_date}'"

    c.execute(DELETE_QUERY)


for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO holdings_btc_bfill_us VALUES ("
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
