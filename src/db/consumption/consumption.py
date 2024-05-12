import logging
import sqlite3
from utils.config import CONSUMPTION_SCHEMA_PATH as PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(PATH)
c = conn.cursor()

LOGGER.info("Creating schema CONSUMPTION at:")
LOGGER.info(f"{PATH}")

##################################################################
#                           PRODUCTS
##################################################################

PRODUCTS_CREATE = """

CREATE TABLE IF NOT EXISTS products (

    ticker      TEXT    NOT NULL    PRIMARY KEY,
    type        TEXT    NOT NULL    CHECK (type in ('etf', 'etp')),
    asset       TEXT    NOT NULL    CHECK (asset in ('BTC')),
    firm        TEXT    NOT NULL,
    country     TEXT                CHECK (country in ('USA'))

)

"""

PRODUCTS_INSERT = """

INSERT INTO products VALUES

    ('ARKB', 'etp', 'BTC', 'Ark Invest', 'USA'),
    ('BITB', 'etp', 'BTC', 'Bitwise', 'USA'),
    ('BRRR', 'etp', 'BTC', 'Valkyrie', 'USA'),
    ('BTCO', 'etp', 'BTC', 'Invesco', 'USA'),
    ('EZBC', 'etp', 'BTC', 'Franklin Templeton', 'USA'),
    ('FBTC', 'etp', 'BTC', 'Fidelity', 'USA'),
    ('GBTC', 'etp', 'BTC', 'Grayscale', 'USA'),
    ('HODL', 'etp', 'BTC', 'Vaneck', 'USA'),
    ('IBIT', 'etp', 'BTC', 'BlackRock', 'USA')

"""

c.execute(PRODUCTS_CREATE)
c.execute(PRODUCTS_INSERT)
LOGGER.info("Created table PRODUCTS")

conn.commit()
conn.close()
