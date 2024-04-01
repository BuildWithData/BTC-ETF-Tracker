from datetime import date
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
from time import sleep


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)


MAX_RETRIES = 10
RETRY_SLEEP = 0.5 # minutes

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

run_date = date.today().isoformat()

for S in services:

    i = S(run_date)
    n = 1

    while n <= MAX_RETRIES:

        LOGGER.info(f"GET {n}: {i}")
        try:
            i.scrape()
            LOGGER.info("Success")
            break

        except:
            LOGGER.warning("Failed")
            _seconds = int(RETRY_SLEEP * 60)
            LOGGER.info(f"Sleeping for {_seconds} seconds")
            n += 1
            sleep(RETRY_SLEEP * 30)
