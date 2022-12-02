import dash
from dash import dcc
from dash import html
import plotly.express as px
import pandas as pd

def test():
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    server = app.server



    #Create dataframes based on the stored csv files
    df_nasdaq = pd.read_csv('app/data/^IXIC_data_yahoo.csv')
    df_sp500 = pd.read_csv('./data/^GSPC_data_yahoo.csv')
    df_coca_cola = pd.read_csv('./data/KO_data_yahoo.csv')
    df_nvidia = pd.read_csv('./data/NVDA_data_yahoo.csv')
    df_gold = pd.read_csv('./data/GC=F_data_yahoo.csv')

    # Create figures for the attributes "Close" and "Volume"
    nd_fig1 = px.bar(df_nasdaq, x="Date", y="Close", barmode="group")
    nd_fig2 = px.bar(df_nasdaq, x="Date", y="Volume", barmode="group")
    sp_fig1 = px.bar(df_sp500, x="Date", y="Close", barmode="group")
    sp_fig2 = px.bar(df_sp500, x="Date", y="Volume", barmode="group")
    cc_fig1 = px.bar(df_coca_cola, x="Date", y="Close", barmode="group")
    cc_fig2 = px.bar(df_coca_cola, x="Date", y="Volume", barmode="group")
    nv_fig1 = px.bar(df_nvidia, x="Date", y="Close", barmode="group")
    nv_fig2 = px.bar(df_nvidia, x="Date", y="Volume", barmode="group")
    gd_fig1 = px.bar(df_gold, x="Date", y="Close", barmode="group")
    gd_fig2 = px.bar(df_gold, x="Date", y="Volume", barmode="group")

    #fig2 = px.bar(df2, x="Date", y="Volume", barmode="group")
    #fig3 = px.bar(df3, x="Date", y="Close", barmode="group")

    app.layout = html.Div(children=[

        # NASDAQ
        ########

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
        ]),

        # S&P500
        #########

        html.Div([
            html.H1(children='S&P 500 Close data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='S&P500_Close',
                figure=sp_fig1
            ),
        ]),

        html.Div([
            html.H1(children='S&P500 Volume data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='S&P500_Volume',
                figure=sp_fig2
            ),
        ]),

        # Coca Cola
        ############

        html.Div([
            html.H1(children='Coca Cola Close data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='Coca_Cola_Close',
                figure=cc_fig1
            ),
        ]),

        html.Div([
            html.H1(children='Coca Cola Volume data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='Coca Cola_Volume',
                figure=cc_fig2
            ),
        ]),

        # NVIDIA
        ########

        html.Div([
            html.H1(children='NVIDIA Close data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='NVIDIA_Close',
                figure=nv_fig1
            ),
        ]),

        html.Div([
            html.H1(children='NVIDIA Volume data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='NVIDIA_Volume',
                figure=nv_fig2
            ),
        ]),

        # Gold
        #######

        html.Div([
            html.H1(children='Gold Close data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='Gold_Close',
                figure=gd_fig1
            ),
        ]),

        html.Div([
            html.H1(children='Gold Volume data'),

            html.Div(children='''
            Historical Data ingested from Yahoo! Finance
        '''),

            dcc.Graph(
                id='Gold_Volume',
                figure=gd_fig2
            ),
        ]),
    ])

    if __name__ == '__main__':
        from waitress import server
        app.run(host='0.0.0.0', debug=True, port=8050)

test()
