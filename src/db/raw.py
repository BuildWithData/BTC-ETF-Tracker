import logging
import sqlite3
from utils.config import RAW_SCHEMA_PATH as PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(PATH)
c = conn.cursor()

LOGGER.info("Creating schema RAW at:")
LOGGER.info(f"{PATH}")

##################################################################
#                           ARKB
##################################################################

CREATE_ARKB_HTML = """

CREATE TABLE IF NOT EXISTS arkb_html (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_cap              REAL,
    daily_share_volume      INT,
    n_shares                INT,
    market_price            REAL,

    PRIMARY KEY (ref_date)
)
"""

CREATE_ARKB_CSV = """

CREATE TABLE IF NOT EXISTS arkb_csv (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    n_coins                 REAL,
    btc_ref_price           REAL,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_ARKB_HTML)
c.execute(CREATE_ARKB_CSV)

c.execute("INSERT into arkb_html VALUES ('','2024-01-09',0,0,0,0)")
c.execute("INSERT into arkb_html VALUES ('','2024-01-10',0,0,0,0)")
c.execute("INSERT into arkb_csv VALUES ('','2024-01-09',0,0)")
c.execute("INSERT into arkb_csv VALUES ('','2024-01-10',0,0)")

LOGGER.info("Created table ARKB_HTML")
LOGGER.info("Created table ARKB_CSV")

##################################################################
#                           BIBT
##################################################################

CREATE_BIBT_FUND_DETAILS = """

CREATE TABLE IF NOT EXISTS bibt_fund_details (

    file_name                       TEXT    NOT NULL,
    ref_date                        TEXT    NOT NULL,
    market_cap                      REAL,
    daily_shares_volume_ref_date    TEXT    NOT NULL,
    daily_shares_volume             INTEGER,
    n_shares                        INTEGER,

    PRIMARY KEY (ref_date, daily_shares_volume_ref_date)
)

"""

CREATE_BIBT_PREMIUM_DISCOUNT = """

CREATE TABLE IF NOT EXISTS bibt_premium_discount (

    file_name       TEXT    NOT NULL,
    ref_date        TEXT    NOT NULL,
    closing_price   REAL,

    PRIMARY KEY (ref_date)
)

"""

CREATE_BIBT_FUND_HOLDINGS = """

CREATE TABLE IF NOT EXISTS bibt_fund_holdings (

    file_name       TEXT    NOT NULL,
    ref_date        TEXT    NOT NULL,
    btc_in_trust    REAL,

    PRIMARY KEY (ref_date)
)

"""

CREATE_BIBT_BTCOIN_HOLDING_ADDRESSES = """

CREATE TABLE IF NOT EXISTS bibt_holding_addresses (

)

"""

c.execute(CREATE_BIBT_FUND_DETAILS)
c.execute(CREATE_BIBT_PREMIUM_DISCOUNT)
c.execute(CREATE_BIBT_FUND_HOLDINGS)
#c.execute(CREATE_BIBT_BTCOIN_HOLDING_ADDRESSES)

c.execute("INSERT into bibt_fund_details VALUES ('','2024-01-09',0,'2024-01-09',0,0)")
c.execute("INSERT into bibt_fund_details VALUES ('','2024-01-10',0,'2024-01-10',0,0)")
c.execute("INSERT into bibt_premium_discount VALUES ('','2024-01-10',0)")
c.execute("INSERT into bibt_fund_holdings VALUES ('','2024-01-10',0)")

LOGGER.info("Created table BIBT_FUND_DETAILS")
LOGGER.info("Created table BIBT_PREMIUM_DISCOUNT")
LOGGER.info("Created table BIBT_FUND_HOLDINGS")

##################################################################
#                           BRRR
##################################################################

CREATE_BRRR_FUND_SUMMARY = """

CREATE TABLE IF NOT EXISTS brrr_fund_summary (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    daily_traded_volume     INT,
    n_shares                INT,
    market_price            REAL,
    btc_ref_price           REAL,

    PRIMARY KEY (ref_date)
)
"""

CREATE_BRRR_HOLDINGS = """

CREATE TABLE IF NOT EXISTS brrr_holdings (

    file_name       TEXT    NOT NULL,
    ref_date        TEXT    NOT NULL,
    market_cap      REAL,
    n_coins         REAL,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_BRRR_FUND_SUMMARY)
c.execute(CREATE_BRRR_HOLDINGS)

c.execute("INSERT into brrr_fund_summary VALUES ('','2024-01-09',Null,Null,0,0)")
c.execute("INSERT into brrr_fund_summary VALUES ('','2024-01-10',Null,Null,0,0)")
c.execute("INSERT into brrr_holdings VALUES ('','2024-01-09',0,0)")
c.execute("INSERT into brrr_holdings VALUES ('','2024-01-10',0,0)")

LOGGER.info("Created table BRRR_FUND_SUMMARY")
LOGGER.info("Created table BRRR_HOLDINGS")

##################################################################
#                           BIBT
##################################################################

CREATE_IBIT = """

CREATE TABLE IF NOT EXISTS ibit (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_cap              INT,
    daily_traded_volume     INT,
    n_shares                INT,
    closing_price           REAL,
    btc_ref_price           REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_IBIT)

c.execute("INSERT into ibit VALUES ('','2024-01-09',0,0,0,0,0,0)")
c.execute("INSERT into ibit VALUES ('','2024-01-10',0,0,0,0,0,0)")

LOGGER.info("Created table IBIT")

##################################################################
#                          BTCO
##################################################################

CREATE_BTCO = """

CREATE TABLE IF NOT EXISTS btco (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_cap              REAL,
    daily_traded_volume     REAL,
    n_shares                INT,
    closing_price           REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_BTCO)

c.execute("INSERT into btco VALUES ('','2024-01-09',0,0,0,0,0)")
c.execute("INSERT into btco VALUES ('','2024-01-10',0,0,0,0,0)")

LOGGER.info("Created table BTCO")

##################################################################
#                           EZBC
##################################################################

CREATE_EZBC_HEADER = """

CREATE TABLE IF NOT EXISTS ezbc_header (

    file_name           TEXT    NOT NULL,
    ref_date            TEXT    NOT NULL,
    market_price        REAL,

    PRIMARY KEY (ref_date)
)
"""

CREATE_EZBC_FUND_INFORMATION = """

CREATE TABLE IF NOT EXISTS ezbc_fund_information (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_cap              REAL,
    daily_share_volume      INT,
    n_shares                INT,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_EZBC_HEADER)
c.execute(CREATE_EZBC_FUND_INFORMATION)

c.execute("INSERT into ezbc_header VALUES ('','2024-01-09',0)")
c.execute("INSERT into ezbc_header VALUES ('','2024-01-10',0)")
c.execute("INSERT into ezbc_fund_information VALUES ('','2024-01-09',0,0,0,0)")
c.execute("INSERT into ezbc_fund_information VALUES ('','2024-01-10',0,0,0,0)")

LOGGER.info("Created table EZBC_HEADER")
LOGGER.info("Created table EZBC_FUND_INFORMATION")

##################################################################
#                          FBTC
##################################################################

CREATE_FBTC_HTML = """

CREATE TABLE IF NOT EXISTS fbtc_html (

    file_name           TEXT    NOT NULL,
    ref_date            TEXT    NOT NULL,
    market_cap          REAL,
    daily_share_volume  INT,
    n_shares            INT,
    market_price        REAL,

    PRIMARY KEY (ref_date)
)
"""

CREATE_FBTC_XLS = """

CREATE TABLE IF NOT EXISTS fbtc_xls (

    file_name           TEXT    NOT NULL,
    ref_date            TEXT    NOT NULL,
    n_coins             REAL,
    btc_ref_price       REAL,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_FBTC_HTML)
c.execute(CREATE_FBTC_XLS)

c.execute("INSERT into fbtc_html VALUES ('','2024-01-09',0,0,0,0)")
c.execute("INSERT into fbtc_html VALUES ('','2024-01-10',0,0,0,0)")
c.execute("INSERT into fbtc_xls VALUES ('','2024-01-09',0,0)")
c.execute("INSERT into fbtc_xls VALUES ('','2024-01-10',0,0)")

LOGGER.info("Created table FBTC_HTML")
LOGGER.info("Created table FBTC_XLS")

##################################################################
#                          GBTC
##################################################################

CREATE_GBTC_KEY_FUND_INFORMATION = """

CREATE TABLE IF NOT EXISTS gbtc_key_fund_information (

    file_name           TEXT    NOT NULL,
    ref_date            TEXT    NOT NULL,
    market_cap          REAL,
    n_shares            INT,
    n_coins             REAL,

    PRIMARY KEY (ref_date)
)
"""

CREATE_GBTC_DAILY_PERFORMANCE = """

CREATE TABLE IF NOT EXISTS gbtc_daily_performance (

    file_name                       TEXT    NOT NULL,
    ref_date                        TEXT    NOT NULL,
    market_price                    REAL,
    daily_share_volume_traded       INT,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_GBTC_KEY_FUND_INFORMATION)
c.execute(CREATE_GBTC_DAILY_PERFORMANCE)

c.execute("INSERT into gbtc_key_fund_information VALUES ('','2024-01-09',0,0,0)")
c.execute("INSERT into gbtc_key_fund_information VALUES ('','2024-01-10',0,0,0)")
c.execute("INSERT into gbtc_daily_performance VALUES ('','2024-01-09',0,0)")
c.execute("INSERT into gbtc_daily_performance VALUES ('','2024-01-10',0,0)")

LOGGER.info("Created table GBTC_KEY_FUND_INFORMATION")
LOGGER.info("Created table GBTC_DAILY_PERFORMANCE")

##################################################################
#                          HODL
##################################################################

CREATE_HODL = """

CREATE TABLE IF NOT EXISTS hodl (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_cap              REAL,
    n_shares                INT,
    n_coins                 REAL,
    market_price            REAL,
    daily_volume_traded     REAL,

    PRIMARY KEY (ref_date)
)
"""

c.execute(CREATE_HODL)

c.execute("INSERT into hodl VALUES ('','2024-01-09',0,0,0,0,0)")
c.execute("INSERT into hodl VALUES ('','2024-01-10',0,0,0,0,0)")

LOGGER.info("Created table HODL")

##################################################################
#                          AB9042
##################################################################

CREATE_AB9042_XLSX = """

CREATE TABLE IF NOT EXISTS ab9042_xlsx (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    total_nav               REAL,
    cash                    REAL,
    market_price            REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_AB9042_XLSX)
c.execute("INSERT into ab9042_xlsx VALUES ('','2024-04-29',0,0,0,0)")
LOGGER.info("Created table AB9042_XLSX")

##################################################################
#                          AE9046
##################################################################

CREATE_AE9046_XLSX = """

CREATE TABLE IF NOT EXISTS ae9046_xlsx (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    total_nav               REAL,
    cash                    REAL,
    market_price            REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_AE9046_XLSX)
c.execute("INSERT into ae9046_xlsx VALUES ('','2024-04-29',0,0,0,0)")
LOGGER.info("Created table AE9046_XLSX")

##################################################################
#                          BB9008
##################################################################

CREATE_BB9008_XLSX = """

CREATE TABLE IF NOT EXISTS bb9008_xlsx (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_value            REAL,
    market_price            REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_BB9008_XLSX)
c.execute("INSERT into bb9008_xlsx VALUES ('','2024-04-29',0,0,0)")
LOGGER.info("Created table BB90082_XLSX")

##################################################################
#                          BE9009
##################################################################

CREATE_BE9009_XLSX = """

CREATE TABLE IF NOT EXISTS be9009_xlsx (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    market_value            REAL,
    market_price            REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_BE9009_XLSX)
c.execute("INSERT into be9009_xlsx VALUES ('','2024-04-29',0,0,0)")
LOGGER.info("Created table BE9009_XLSX")

####################################################################
conn.commit()
conn.close()
