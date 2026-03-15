from dash import Dash
from dash import dash_table
from dash import dcc
from dash import html
import pandas as pd
from visualization.hk import current_holdings, daily_inflows
from visualization.hk import cumulative_inflows_btc, cumulative_inflows_eth
from visualization.hk import total_daily_inflows_btc, total_daily_inflows_eth

pd.options.mode.chained_assignment = None

app = Dash(__name__)

app.layout = html.Div([

    ################################################
    html.H1(
        children='Hong Kong ETFs - Total Holdings [Coins]',
        style={"textAlign": "center"}
    ),
    dash_table.DataTable(
        data=current_holdings().to_dict('records') if not current_holdings().empty else [],
        page_size=5,
    ),

    ################################################
    html.H3(
        children='Daily Inflows [Coins]',
        style={"textAlign": "center", "marginTop": "30px"}
    ),
    dash_table.DataTable(
        data=daily_inflows().to_dict('records') if not daily_inflows().empty else [],
        page_size=5
    ),

    ################################################
    html.H2(
        children='Bitcoin (BTC) Sub-Dashboard',
        style={"textAlign": "center", "marginTop": "50px", "borderTop": "2px solid #ccc", "paddingTop": "20px"}
    ),

    dcc.Graph(
        figure=cumulative_inflows_btc(),
    ),

    dcc.Graph(
        figure=total_daily_inflows_btc(),
    ),

    ################################################
    html.H2(
        children='Ethereum (ETH) Sub-Dashboard',
        style={"textAlign": "center", "marginTop": "50px", "borderTop": "2px solid #ccc", "paddingTop": "20px"}
    ),

    dcc.Graph(
        figure=cumulative_inflows_eth(),
    ),

    dcc.Graph(
        figure=total_daily_inflows_eth(),
    ),
])

if __name__ == '__main__':
    app.run(debug=True)
