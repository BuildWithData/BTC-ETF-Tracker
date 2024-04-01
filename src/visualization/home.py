import calendar
from datetime import date
from pandas import DataFrame
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure
import plotly.graph_objects as go
import sqlite3
from utils.config import CONSUMPTION_SCHEMA_PATH
from utils.constants import TICKERS
from utils.constants import TICKERS_NO_BTCO
import yfinance as yf


conn = sqlite3.connect(CONSUMPTION_SCHEMA_PATH)
c = conn.cursor()


def current_holdings() -> DataFrame:
    """
    Table with current holdings [BTC] at most recent available date
    """

    QUERY = "select * from holdings_btc_bfill order by ref_date desc limit 1"
    current_holdings = pd.DataFrame(c.execute(QUERY), columns=["Date"] + TICKERS + ["TOTAL"])
    current_holdings = current_holdings.drop("BTCO", axis=1)
    current_holdings = current_holdings.round(2)

    return current_holdings


def daily_inflows() -> DataFrame:
    """
    Table with daily inflows [BTC]
    """

    QUERY = "select * from inflows_btc_bxfill"
    inflows = pd.DataFrame(c.execute(QUERY), columns=["Date"] + TICKERS_NO_BTCO + ["TOTAL"])

    monday2wednseday = [
        date.fromisoformat("2024-01-08"),
        date.fromisoformat("2024-01-09"),
        date.fromisoformat("2024-01-10")
    ]

    monday2wednseday = pd.DataFrame(monday2wednseday, columns=["Date"])
    inflows = pd.concat([monday2wednseday, inflows]).reset_index(drop=True)

    inflows["Date"] = inflows.Date.apply(lambda s: date.fromisoformat(s) if isinstance(s, str) else s)
    inflows["day"] = inflows.Date.apply(lambda d: calendar.day_name[d.weekday()] if isinstance(d, date) else d)

    inflows = inflows.round(2)

    return inflows


def cumulative_inflows() -> Figure:
    """
    Graph with cumulative daily inflows [BTC]
    GBTC not included
    """

    QUERY = "select * from inflows_btc_bxfill"
    inflows = pd.DataFrame(c.execute(QUERY), columns=["Date"] + TICKERS_NO_BTCO + ["TOTAL"])
    inflows = inflows.fillna(0) # btmx missing data for 2024-01-15, 2024-02-19

    cum = inflows[["Date"]]
    for t in TICKERS_NO_BTCO:
        cum[t] = inflows[t].cumsum()

    # prase to schema required by plotly
    out = cum[["Date"]]
    out["TICKER"] = "ARKB"
    out["BTC"] = cum["ARKB"]

    for t in TICKERS_NO_BTCO:
        if t not in ("ARKB", "GBTC"):
            tmp = cum[["Date"]]
            tmp["TICKER"] = t
            tmp["BTC"] = cum[t]

            out = pd.concat([out, tmp])

    fig = px.area(out, x="Date", y="BTC", color="TICKER")
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

    return fig


def cumulative_outflows_GBTC() -> Figure:
    """
    Graph with cumulative daily outflows [BTC] for GBTC
    """

    QUERY = "select ref_date, GBTC from inflows_btc_bxfill"
    outflows = pd.DataFrame(c.execute(QUERY), columns=["Date", "GBTC"])
    outflows = outflows.fillna(0) # btmx missing data for 2024-01-15, 2024-02-19
    outflows["GBTC"] = outflows.GBTC.cumsum()

    fig = px.area(outflows, x="Date", y="GBTC")
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

    return fig


def total_daily_inflows() -> Figure:
    """
    Bar graph with daily inflows/outflows [BTC]
    and 5 five day moving average
    """

    QUERY = "select ref_date, total from inflows_btc_bxfill"
    total_daily_inflows = pd.DataFrame(c.execute(QUERY), columns=["Date", "TOTAL"])
    total_daily_inflows = total_daily_inflows.fillna(0) # btmx missing data for 2024-01-15, 2024-02-19

    QUERY = "select * from inflows_btc_sma5"
    sma5 = pd.DataFrame(c.execute(QUERY), columns=["Date"] + TICKERS_NO_BTCO + ["TOTAL"])
    sma5 = sma5.fillna(0)

    fig = go.Figure()

    inflows = total_daily_inflows[total_daily_inflows.TOTAL > 0]
    inflows = go.Bar(x=inflows.Date, y=inflows.TOTAL, marker_color='green', name='inflow')

    outflows = total_daily_inflows[total_daily_inflows.TOTAL < 0]
    outflows = go.Bar(x=outflows.Date, y=outflows.TOTAL, marker_color='red', name='outlfow')

    fig.add_trace(inflows)
    fig.add_trace(outflows)

    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    fig.add_trace(go.Scatter(x=sma5.Date, y=sma5.TOTAL, name="avg 5 days inflow", marker={"color": "orange"}))

    return fig


def btc_daily_price() -> Figure:
    """
    Graph with btc daily closing price [USD] and candles
    """

    today = date.today().isoformat()
    price = yf.download(tickers='BTC-USD', start='2024-01-11', end=today)
    price = price.reset_index().rename({"Close": "Price-Close"}, axis=1)
    price = price.reset_index()

    candles = go.Candlestick(
        x=price.Date,
        open=price.Open,
        high=price.High,
        low=price.Low,
        close=price["Price-Close"]
    )

    closing_price = go.Scatter(
        x=price.Date,
        y=price["Price-Close"],
        name="Price Close",
        marker={"color": "orange"}
    )

    fig = go.Figure(candles)
    fig.add_trace(closing_price)

    #fig.update_layout(margin=dict(l=1, r=1, t=1, b=1))

    fig.update_layout(xaxis_rangeslider_visible=False)

    return fig
