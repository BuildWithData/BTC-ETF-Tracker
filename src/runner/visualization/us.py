from dash import Dash
from dash import dash_table
from dash import dcc
from dash import html
import pandas as pd
from visualization.us import btc_daily_price
from visualization.us import cumulative_inflows
from visualization.us import cumulative_outflows_GBTC
from visualization.us import current_holdings
from visualization.us import daily_inflows
from visualization.us import total_daily_inflows


pd.options.mode.chained_assignment = None

app = Dash(__name__)

app.layout = html.Div([

    ################################################
    html.H1(
        children='Total Holdings [BTC]',
        style={"textAlign": "center"}
    ),
    dash_table.DataTable(
        data=current_holdings().to_dict('records'),
        page_size=5,
    ),

    ################################################
    html.H3(
        children='Daily Inflows [BTC]',
        style={"textAlign": "center"}
    ),
    dash_table.DataTable(
        data=daily_inflows().to_dict('records'),
        page_size=5
    ),

    ################################################
    html.H3(
        children='ETF Cumulative Inflow [BTC]',
        style={"textAlign": "center"}
    ),

    dcc.Graph(
        figure=cumulative_inflows(),
    ),

    ################################################
    html.H3(
        children='GBTC Cumulative OutFlow [BTC]',
        style={"textAlign": "center"}
    ),

    dcc.Graph(
        figure=cumulative_outflows_GBTC(),
    ),

    ################################################
    html.H3(
        children='Total Daily Inflow [BTC]',
        style={"textAlign": "center"}
    ),

    html.P(
        children='Data for Jan 15 and Feb 19 not available',
        style={"textAlign": "center"}
    ),

    html.P(
        children='Outflow on 23 Feb due to data reconcilation with BTMX',
        style={"textAlign": "center"}
    ),

    dcc.Graph(
        figure=total_daily_inflows(),
    ),

    ################################################
    html.H3(
        children='Daily Price BTC [$]',
        style={"textAlign": "center"}
    ),

    dcc.Graph(
        figure=btc_daily_price(),
    )
])


if __name__ == '__main__':
    app.run(debug=True)
