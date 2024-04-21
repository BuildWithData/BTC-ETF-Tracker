# db/util.py
# Description: This file contains the utility functions for the database.

import sqlite3
from utils.config import CONSUMPTION_SCHEMA_PATH as PATH
from db.util import load_sql_file

def load_sql_file(filename):
    with open(filename, 'r') as file:
        return file.read()

def start_connection(LOGGER, starting_message=True):
    conn = sqlite3.connect(PATH)
    c = conn.cursor()

    LOGGER.info(starting_message)
    LOGGER.info(f"{PATH}")
    return conn

def end_connection(conn):
    conn.commit()
    conn.close()