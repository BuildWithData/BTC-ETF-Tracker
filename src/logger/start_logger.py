# logger/start_logger.py
# Description: This file contains the function to start the logger.

import logging

def start_logger():
    LOGGER = logging.getLogger()
    s_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    s_handler.setFormatter(formatter)
    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(s_handler)
    return LOGGER