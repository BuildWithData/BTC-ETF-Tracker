import argparse
from datetime import date
from datetime import timedelta
import logging
from product.us import TICKERS as US_TICKERS
from product.hk import BTC_TICKERS as HK_BTC_TICKERS
from product.hk import ETH_TICKERS as HK_ETH_TICKERS
import sqlite3
from utils.config import RAW_SCHEMA_PATH


LOGGER = logging.getLogger()
s_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
s_handler.setFormatter(formatter)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(s_handler)

parser = argparse.ArgumentParser(description="update all tables in schema raw")
parser.add_argument("-d", "--date", help="target date", required=False)

conn = sqlite3.connect(RAW_SCHEMA_PATH)
c = conn.cursor()

services = US_TICKERS
services = HK_BTC_TICKERS + HK_ETH_TICKERS

################
# INPUTS
args = parser.parse_args()
ref_date = args.date

if ref_date is not None:
    target_dates = [date.fromisoformat(ref_date)]

else:
    start = date.fromisoformat("2024-02-25")
    end = date.today()
    n_days = (end - start).days + 1

    target_dates = [start + timedelta(d) for d in range(n_days)]


for date_ in target_dates:

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
