# db/create_tables.py
# Description: This file contains the functions to create the tables in the database.

from db.util import load_sql_file

def create_consumption_schema(logger, conn):
    create_table_products(logger, conn)
    create_table_holdings_btc(logger, conn)
    create_table_holdings_btc_bfill(logger, conn)
    create_table_inflows_btc(logger, conn)
    create_table_inflows_btc_bfill(logger, conn)
    create_table_inflows_btc_bfill_inline(logger, conn)
    create_table_inflows_btc_bxfill(logger, conn)
    create_table_inflows_btc_sma5(logger, conn)


def create_table_products(logger, conn):
    PRODUCTS_CREATE = load_sql_file("db/queries/products_create.sql")
    PRODUCTS_INSERT = load_sql_file("db/queries/products_insert.sql")

    conn.execute(PRODUCTS_CREATE)
    conn.execute(PRODUCTS_INSERT)
    logger.info("Created table PRODUCTS")

def create_table_holdings_btc(logger, conn):
    HOLDINGS_BTC_CREATE = load_sql_file("db/queries/holdings_btc_create.sql")
    conn.execute(HOLDINGS_BTC_CREATE)
    logger.info("Created table HOLDINGS_BTC")

def create_table_holdings_btc_bfill(logger, conn):
    HOLDINGS_BTC_BFILL_CREATE = load_sql_file("db/queries/holdings_btc_bfill_create.sql")
    conn.execute(HOLDINGS_BTC_BFILL_CREATE)
    logger.info("Created table HOLDINGS_BTC_BFILL")

def create_table_inflows_btc(logger, conn):
    INFLOWS_BTC_CREATE = load_sql_file("db/queries/inflows_btc_create.sql")
    conn.execute(INFLOWS_BTC_CREATE)
    logger.info("Created table INFLOWS_BTC")

def create_table_inflows_btc_bfill(logger, conn):
    INFLOWS_BTC_BFILL_CREATE = load_sql_file("db/queries/inflows_btc_bfill_create.sql")
    conn.execute(INFLOWS_BTC_BFILL_CREATE)
    logger.info("Created table INFLOWS_BTC_BFILL")

def create_table_inflows_btc_bfill_inline(logger, conn):
    INFLOWS_BTC_BFILL_CREATE = load_sql_file("db/queries/inflows_btc_bfill_create.sql")
    conn.execute(INFLOWS_BTC_BFILL_CREATE)
    logger.info("Created table INFLOWS_BTC_BFILL")

def create_table_inflows_btc_bxfill(logger, conn):
    INFLOWS_BTC_BXFILL_CREATE = load_sql_file("db/queries/inflows_btc_bxfill_create.sql")
    conn.execute(INFLOWS_BTC_BXFILL_CREATE)
    logger.info("Created table INFLOWS_BTC_BXFILL")

def create_table_inflows_btc_sma5(logger, conn):
    INFLOWS_BTC_SMA5_CREATE = load_sql_file("db/queries/inflows_btc_sma5_create.sql")
    conn.execute(INFLOWS_BTC_SMA5_CREATE)
    logger.info("Created table INFLOWS_BTC_SMA5")