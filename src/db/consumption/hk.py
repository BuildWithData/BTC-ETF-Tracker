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
#                           HOLDINGS
##################################################################

HOLDINGS_CREATE = """

CREATE TABLE IF NOT EXISTS holdings (

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

c.execute(HOLDINGS_CREATE)
LOGGER.info("Created table HOLDINGS")

##################################################################
#                           HOLDINGS_BFILL
##################################################################

HOLDINGS_BFILL_CREATE = """

CREATE TABLE IF NOT EXISTS holdings_bfill (

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

c.execute(HOLDINGS_BFILL_CREATE)
LOGGER.info("Created table HOLDINGS_BFILL")

##################################################################
#                           INFLOWS
##################################################################

INFLOWS_CREATE = """

CREATE TABLE IF NOT EXISTS inflows (

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

c.execute(INFLOWS_CREATE)
LOGGER.info("Created table INFLOWS")

##################################################################
#                           INFLOWS_BFILL
##################################################################

INFLOWS_BFILL_CREATE = """

CREATE TABLE IF NOT EXISTS inflows_bfill (

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

c.execute(INFLOWS_BFILL_CREATE)
LOGGER.info("Created table INFLOWS_BFILL")

##################################################################
#                           INFLOWS_SMA5
##################################################################

INFLOWS_SMA5_CREATE = """

CREATE TABLE IF NOT EXISTS inflows_sma5 (

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

c.execute(INFLOWS_SMA5_CREATE)
LOGGER.info("Created table INFLOWS_SMA5")

###################################################################
conn.commit()
conn.close()
