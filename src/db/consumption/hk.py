import logging
import sqlite3
from utils.config import CONSUMPTION_HK_SCHEMA_PATH as PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(PATH)
c = conn.cursor()

LOGGER.info("Creating schema CONSUMPTION.HK at:")
LOGGER.info(f"{PATH}")

##################################################################
#                           HOLDINGS_BTC
##################################################################

HOLDINGS_BTC_CREATE = """

CREATE TABLE IF NOT EXISTS holdings_btc (

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    AB9042      REAL,
    AE9046      REAL,
    BB9008      REAL,
    BE9009      REAL,
    HB9439      REAL,
    HE9179      REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(HOLDINGS_BTC_CREATE)
LOGGER.info("Created table HOLDINGS_BTC")

##################################################################
#                           HOLDINGS_BTC_BFILL
##################################################################

HOLDINGS_BTC_CREATE = """

CREATE TABLE IF NOT EXISTS holdings_btc_bfill (

    ref_date        TEXT    NOT NULL,
    week            TEXT    NOT NULL,
    day             TEXT    NOT NULL,
    AB9042          REAL,
    AE9046          REAL,
    BB9008          REAL,
    BE9009          REAL,
    HB9439          REAL,
    HE9179          REAL,
    TOTAL_BTC       REAL,
    TOTAL_ETH       REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(HOLDINGS_BTC_CREATE)
LOGGER.info("Created table HOLDINGS_BTC_BFILL")

###################################################################
conn.commit()
conn.close()
