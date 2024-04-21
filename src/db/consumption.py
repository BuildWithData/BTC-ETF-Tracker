# db/consumption.py
# Description: This file contains the utility functions for the database.

from utils.config import CONSUMPTION_SCHEMA_PATH as PATH
from logger.start_logger import start_logger
from db.util import start_connection, end_connection
from db.create_tables import create_consumption_schema

starting_message = "Creating schema CONSUMPTION at:"


LOGGER = start_logger()

conn = start_connection(LOGGER)

create_consumption_schema(LOGGER, conn)

end_connection(conn)
