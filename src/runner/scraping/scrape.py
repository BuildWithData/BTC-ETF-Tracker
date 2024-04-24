import argparse
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

# TODO: str(s) not working
_TICKERS = [s.__str__() for s in services]

parser = argparse.ArgumentParser(
    description="scrape data from issuer's websites, available tickers are:\n\n{}".format(", ".join(_TICKERS)),
    formatter_class=argparse.RawTextHelpFormatter
)
parser.add_argument(
    "-t",
    "--tickers",
    help="scrape data for target tickers only",
    nargs="+",
    required=False
)
parser.add_argument(
    "-it",
    "--ignore-tickers",
    help="skip scraping for target tickers",
    nargs="+",
    required=False
)

MAX_RETRIES = 10
RETRY_SLEEP = 0.5 # minutes


##############
# INPUTS
args = parser.parse_args()
tickers = args.tickers
ignore_tickers = args.ignore_tickers

if tickers is not None:

    for t in tickers:
        if t not in _TICKERS:
            raise ValueError(f"Invalid ticker: {t}")

    # TODO: str(s) not working
    services = [s for s in services if s.__str__() in tickers]

if ignore_tickers is not None:

    for t in ignore_tickers:
        if t not in _TICKERS:
            raise ValueError(f"Invalid ticker: {t}")

    # TODO: str(s) not working
    services = [s for s in services if s.__str__() not in ignore_tickers]

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
