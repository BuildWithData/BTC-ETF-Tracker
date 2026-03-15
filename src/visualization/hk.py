import sqlite3
import pandas as pd
from pandas import DataFrame
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objects import Figure
from utils.config import CONSUMPTION_HK_SCHEMA_PATH
from utils.constants import HK_BTC_TICKERS, HK_ETH_TICKERS

conn = sqlite3.connect(CONSUMPTION_HK_SCHEMA_PATH)
c = conn.cursor()


def current_holdings() -> DataFrame:
    """
    Table with current holdings at most recent available date
    """
    from visualization.common import current_holdings as ch
    return ch(c, table_name="holdings_bfill")


def daily_inflows() -> DataFrame:
    """
    Table with daily inflows
    """
    QUERY = "select * from inflows"
    cursor = c.execute(QUERY)
    cols = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=cols)

    if df.empty:
        return df

    df = df.rename({"ref_date": "Date"}, axis=1)
    for col in ["week", "day"]:
        if col in df.columns:
            df = df.drop(col, axis=1)

    return df.round(2)


def cumulative_inflows_btc() -> Figure:
    """
    Graph with cumulative daily inflows for BTC tickers
    """
    QUERY = "select * from inflows"
    cursor = c.execute(QUERY)
    cols = [desc[0] for desc in cursor.description]
    inflows = pd.DataFrame(cursor.fetchall(), columns=cols)

    if inflows.empty:
        return go.Figure()

    inflows = inflows.fillna(0)
    cum = inflows[["ref_date"]]
    tickers_present = [t for t in HK_BTC_TICKERS if t in inflows.columns]
    
    for t in tickers_present:
        cum[t] = inflows[t].cumsum()

    out = pd.DataFrame()
    for t in tickers_present:
        tmp = cum[["ref_date"]]
        tmp["TICKER"] = t
        tmp["COINS"] = cum[t]
        out = pd.concat([out, tmp])

    if out.empty:
        return go.Figure()

    fig = px.area(out, x="ref_date", y="COINS", color="TICKER", title="BTC Cumulative Inflow [Coins]")
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], title="Date")
    return fig


def cumulative_inflows_eth() -> Figure:
    """
    Graph with cumulative daily inflows for ETH tickers
    """
    QUERY = "select * from inflows"
    cursor = c.execute(QUERY)
    cols = [desc[0] for desc in cursor.description]
    inflows = pd.DataFrame(cursor.fetchall(), columns=cols)

    if inflows.empty:
        return go.Figure()

    inflows = inflows.fillna(0)
    cum = inflows[["ref_date"]]
    tickers_present = [t for t in HK_ETH_TICKERS if t in inflows.columns]
    
    for t in tickers_present:
        cum[t] = inflows[t].cumsum()

    out = pd.DataFrame()
    for t in tickers_present:
        tmp = cum[["ref_date"]]
        tmp["TICKER"] = t
        tmp["COINS"] = cum[t]
        out = pd.concat([out, tmp])

    if out.empty:
        return go.Figure()

    fig = px.area(out, x="ref_date", y="COINS", color="TICKER", title="ETH Cumulative Inflow [Coins]")
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], title="Date")
    return fig


def total_daily_inflows_btc() -> Figure:
    """
    Bar graph with daily inflows/outflows for BTC [TOTAL_BTC]
    Includes 5-day moving average if inflows_sma5 is populated
    """
    QUERY = "select ref_date, total_btc from inflows"
    total_daily = pd.DataFrame(c.execute(QUERY), columns=["Date", "TOTAL"])
    total_daily = total_daily.fillna(0)

    fig = go.Figure()

    inflows = total_daily[total_daily.TOTAL > 0]
    inflows_draw = go.Bar(x=inflows.Date, y=inflows.TOTAL, marker_color='green', name='Inflow (BTC)')

    outflows = total_daily[total_daily.TOTAL < 0]
    outflows_draw = go.Bar(x=outflows.Date, y=outflows.TOTAL, marker_color='red', name='Outflow (BTC)')

    fig.add_trace(inflows_draw)
    fig.add_trace(outflows_draw)
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    fig.update_layout(title="Total Daily Inflow BTC [Coins]")

    return fig


def total_daily_inflows_eth() -> Figure:
    """
    Bar graph with daily inflows/outflows for ETH [TOTAL_ETH]
    """
    QUERY = "select ref_date, total_eth from inflows"
    total_daily = pd.DataFrame(c.execute(QUERY), columns=["Date", "TOTAL"])
    total_daily = total_daily.fillna(0)

    fig = go.Figure()

    inflows = total_daily[total_daily.TOTAL > 0]
    inflows_draw = go.Bar(x=inflows.Date, y=inflows.TOTAL, marker_color='green', name='Inflow (ETH)')

    outflows = total_daily[total_daily.TOTAL < 0]
    outflows_draw = go.Bar(x=outflows.Date, y=outflows.TOTAL, marker_color='red', name='Outflow (ETH)')

    fig.add_trace(inflows_draw)
    fig.add_trace(outflows_draw)
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    fig.update_layout(title="Total Daily Inflow ETH [Coins]")

    return fig
