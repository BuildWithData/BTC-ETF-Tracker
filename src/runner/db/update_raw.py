from datetime import date
from datetime import timedelta
import logging
from product.etp.arkb import ARKB
from product.etp.bitb import BITB
from product.etp.brrr import BRRR
from product.etp.btco import BTCO
from product.etp.ezbc import EZBC
from product.etp.fbtc import FBTC
from product.etp.gbtc import GBTC
from product.etp.hodl import HODL
from product.etp.ibit import IBIT
import sqlite3
from utils.config import RAW_SCHEMA_PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

conn = sqlite3.connect(RAW_SCHEMA_PATH)
c = conn.cursor()

services = [

    ARKB,
    BITB,
    BRRR,
    BTCO,
    GBTC,
    EZBC,
    FBTC,
    HODL,
    IBIT

]

start = date.fromisoformat("2024-02-25")
end = date.today()

n_days = (end - start).days + 1

for d in range(n_days):

    date_ = start + timedelta(d)
    LOGGER.info("##############################################")
    LOGGER.info(date_)

    for Service in services:

        try:
            s = Service(date_.isoformat())
            s.read()
            s.extract()
            s.update_db(conn)
        except:
            LOGGER.warning(f"{s} has failed")
