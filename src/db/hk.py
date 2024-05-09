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
#                          AB9042
##################################################################

CREATE_AB9042 = """

CREATE TABLE IF NOT EXISTS ab9042_xlsx (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    total_nav               REAL,
    cash                    INT,
    market_price            REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_AB9042)
c.execute("INSERT into ab9042_xlsx VALUES ('','2024-05-01',0,0,0,0)")
LOGGER.info("Created table AB9042_XLSX")

##################################################################
#                          AE9046
##################################################################

CREATE_AE9046 = """

CREATE TABLE IF NOT EXISTS ae9046_xlsx (

    file_name               TEXT    NOT NULL,
    ref_date                TEXT    NOT NULL,
    total_nav               REAL,
    cash                    INT,
    market_price            REAL,
    n_coins                 REAL,

    PRIMARY KEY (ref_date)
)

"""

c.execute(CREATE_AE9046)
c.execute("INSERT into ae9046_xlsx VALUES ('','2024-05-01',0,0,0,0)")
LOGGER.info("Created table AE9046_XLSX")

####################################################################
conn.commit()
conn.close()
