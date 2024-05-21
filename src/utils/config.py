import os
import yaml


_CONFIG_PATH = "./../config.yaml"


with open(_CONFIG_PATH, "r") as f:
    _config = yaml.safe_load(f)


DATA_BASE_PATH = _config["Path"]["data"]

RAW_PATH = os.path.join(DATA_BASE_PATH, "raw")
PATH_BTMX = os.path.join(RAW_PATH, "external/bitmex/btc_flow.csv")

DATABASE_BASE_PATH = _config["Path"]["db"]
if DATABASE_BASE_PATH == "" or DATABASE_BASE_PATH is None:
    DATABASE_BASE_PATH = os.path.join(DATA_BASE_PATH, "db")

CONSUMPTION_SCHEMA_PATH = os.path.join(DATABASE_BASE_PATH, "consumption", "consumption")
CONSUMPTION_HK_SCHEMA_PATH = os.path.join(DATABASE_BASE_PATH, "consumption", "hk")
CONSUMPTION_US_SCHEMA_PATH = os.path.join(DATABASE_BASE_PATH, "consumption", "us")

RAW_SCHEMA_PATH = os.path.join(DATABASE_BASE_PATH, "raw")
