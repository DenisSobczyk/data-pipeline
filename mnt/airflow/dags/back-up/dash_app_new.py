import dash
from dash import dcc
from dash import html
import plotly.express as px
import pandas as pd


def dash_app():
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    server = app.server

    # Create dataframes based on the stored csv files
    df_nasdaq = pd.read_csv('./data/^IXIC_data_yahoo.csv')

    # Create figures for the attributes "Close" and "Volume"
    nd_fig1 = px.bar(df_nasdaq, x="Date", y="Close", barmode="group")
    nd_fig2 = px.bar(df_nasdaq, x="Date", y="Volume", barmode="group")

    app.layout = html.Div(children=[

        html.Div([
            html.H1(children='Nasdaq Close data'),

            html.Div(children='''
                Historical Data ingested from Yahoo! Finance
            '''),

            dcc.Graph(
                id='Nasdaq_Close',
                figure=nd_fig1
            ),
        ]),
        #
        html.Div([
            html.H1(children='Nasdaq Volume data'),

            html.Div(children='''
                Historical Data ingested from Yahoo! Finance
            '''),

            dcc.Graph(
                id='Nasdaq_Volume',
                figure=nd_fig2
            ),
        ]), ])

    if __name__ == '__main__':
        app.run_server(debug=True)


dash_app()
