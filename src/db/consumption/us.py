import logging
import sqlite3
from utils.config import CONSUMPTION_US_SCHEMA_PATH as PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(PATH)
c = conn.cursor()

LOGGER.info("Creating schema CONSUMPTION.US at:")
LOGGER.info(f"{PATH}")

##################################################################
#                           HOLDINGS_BTC
##################################################################

HOLDINGS_BTC_CREATE = """

CREATE TABLE IF NOT EXISTS holdings_btc (

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,

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

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,
    TOTAL       REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(HOLDINGS_BTC_CREATE)
LOGGER.info("Created table HOLDINGS_BTC_BFILL")

##################################################################
#                           INFLOWS_BTC
##################################################################

INFLOWS_BTC_CREATE = """

CREATE TABLE IF NOT EXISTS inflows_btc (

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,
    TOTAL       REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(INFLOWS_BTC_CREATE)
LOGGER.info("Created table INFLOWS_BTC")

##################################################################
#                           INFLOWS_BTC_BFILL
##################################################################

INFLOWS_BTC_BFILL_CREATE = """

CREATE TABLE IF NOT EXISTS inflows_btc_bfill (

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,
    TOTAL       REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(INFLOWS_BTC_BFILL_CREATE)
LOGGER.info("Created table INFLOWS_BTC_BFILL")

##################################################################
#                           INFLOWS_BTC_BXFILL
##################################################################

INFLOWS_BTC_BXFILL_CREATE = """

CREATE TABLE IF NOT EXISTS inflows_btc_bxfill (

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
--    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,
    TOTAL       REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(INFLOWS_BTC_BXFILL_CREATE)
LOGGER.info("Created table INFLOWS_BTC_BXFILL")

##################################################################
#                           INFLOWS_BTC_SMA5
##################################################################

INFLOWS_BTC_SMA5_CREATE = """

CREATE TABLE IF NOT EXISTS inflows_btc_sma5 (

    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
--    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,
    TOTAL       REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(INFLOWS_BTC_SMA5_CREATE)
LOGGER.info("Created table INFLOWS_BTC_SMA5")

###################################################################
conn.commit()
conn.close()
