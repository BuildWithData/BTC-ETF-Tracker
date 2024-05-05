import logging
import pandas as pd
import sqlite3
from utils.config import CONSUMPTION_US_SCHEMA_PATH
from utils.constants import TICKERS_NO_BTCO as TICKERS


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(CONSUMPTION_US_SCHEMA_PATH)
c = conn.cursor()

#################
# READ
QUERY = "select * from inflows_btc_bxfill"
df = pd.DataFrame(c.execute(QUERY), columns=["ref_date", "week", "day"] + TICKERS + ["TOTAL"])

#################
# PARSE
out = df.fillna(0) # btmx data missing on 2024-01-15, 2024-02-19

for col in TICKERS:
    out[col] = out[col].rolling(5).mean().round(2)

out["TOTAL"] = out.TOTAL.rolling(5).mean().round(2)

##################
# LOAD

CLEAN_TABLE_QUERY = "DELETE FROM inflows_btc_sma5"
c.execute(CLEAN_TABLE_QUERY)

for row in out.itertuples():

    INSERT_QUERY = "INSERT INTO inflows_btc_sma5 VALUES ("
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
