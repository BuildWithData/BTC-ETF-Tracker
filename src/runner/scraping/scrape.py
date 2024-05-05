import argparse
from datetime import date
import logging
from product.us import TICKERS as US_TICKERS
from product.hk import BTC_TICKERS as _HK_BTC_TICKERS
from product.hk import ETH_TICKERS as _HK_ETH_TICKERS
from time import sleep


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

HK_TICKERS = _HK_BTC_TICKERS + _HK_ETH_TICKERS
_US_TICKERS = [s.__str__() for s in US_TICKERS]
_HK_TICKERS = [s.__str__() for s in HK_TICKERS]

_dsc = """

scrape data from issuer's websites, available tickers are:

# US
{}

# HK
{}

""".format(
    ", ".join(_US_TICKERS),
    ", ".join(_HK_TICKERS)
)


parser = argparse.ArgumentParser(
    description=_dsc,
    formatter_class=argparse.RawTextHelpFormatter
)
parser.add_argument(
    "-m",
    "--market",
    help="market where etfs are traded",
    required=True,
    choices=["us", "hk"]
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
market = args.market
tickers = args.tickers
ignore_tickers = args.ignore_tickers

if market == "us":
    services = US_TICKERS
    _TICKERS = _US_TICKERS

elif market == "hk":
    services = HK_TICKERS
    _TICKERS = _HK_TICKERS

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

####################
# SCRAPING

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
