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
    firm        TEXT    NOT NULL,
    country     TEXT                CHECK (country in ('USA', 'CANADA'))

)

"""

PRODUCTS_INSERT = """

INSERT INTO products VALUES

    ('ARKB', 'etp', 'Ark Invest', 'USA'),
    ('BITB', 'etp', 'Bitwise', 'USA'),
    ('BRRR', 'etp', 'Valkyrie', 'USA'),
    ('BTCO', 'etp', 'Invesco', 'USA'),
    ('EZBC', 'etp', 'Franklin Templeton', 'USA'),
    ('FBTC', 'etp', 'Fidelity', 'USA'),
    ('GBTC', 'etp', 'Grayscale', 'USA'),
    ('HODL', 'etp', 'Vaneck', 'USA'),
    ('IBIT', 'etp', 'BlackRock', 'USA')

"""

c.execute(PRODUCTS_CREATE)
c.execute(PRODUCTS_INSERT)
LOGGER.info("Created table PRODUCTS")
