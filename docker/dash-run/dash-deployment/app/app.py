from dash.dependencies import Output, Input, State
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
from flask import Flask
import pandas as pd
import dash

server = Flask(__name__)
app = dash.Dash(server=server, external_stylesheets=[dbc.themes.FLATLY])
app.title = 'Dashboard'

'''
Loading all the Dataframes and creating the figures
to visualize it in HTML format
'''

# Amazon Data (Close, Volume)
df_am = pd.read_csv('/wd/data/AMZN_data_yahoo.csv', usecols=['Date', 'Close', 'Volume'])
df_am['Date'] = pd.to_datetime(df_am['Date'])
fig_am1=px.line(df_am, x='Date', y='Close')
fig_am2=px.line(df_am, x='Date', y='Volume')

# Nvidia Data (Close, Volume)
df_nv = pd.read_csv('/wd/data/NVDA_data_yahoo.csv', usecols=['Date', 'Close', 'Volume'])
df_nv['Date'] = pd.to_datetime(df_nv['Date'])
fig_nv1=px.line(df_nv, x='Date', y='Close')
fig_nv2=px.line(df_nv, x='Date', y='Volume')

# Coca Cola Data (Close, Volume)
df_co = pd.read_csv('/wd/data/KO_data_yahoo.csv', usecols=['Date', 'Close', 'Volume'])
df_co['Date'] = pd.to_datetime(df_co['Date'])
fig_co1=px.line(df_co, x='Date', y='Close')
fig_co2=px.line(df_co, x='Date', y='Volume')

# S&P500 Data (Close, Volume)
df_co = pd.read_csv('/wd/data/^GSPC_data_yahoo.csv', usecols=['Date', 'Close', 'Volume'])
df_co['Date'] = pd.to_datetime(df_co['Date'])
fig_sp1=px.line(df_co, x='Date', y='Close')
fig_sp2=px.line(df_co, x='Date', y='Volume')

# Gold Data (Close, Volume)
df_gd = pd.read_csv('/wd/data/GC=F_data_yahoo.csv', usecols=['Date', 'Close', 'Volume'])
df_gd['Date'] = pd.to_datetime(df_gd['Date'])
fig_gd1=px.line(df_gd, x='Date', y='Close')


app.layout = html.Div(
    id="app-container",
    children=[

        # Amazon
        html.H1("Amazon Stock Prices"),
        html.P("in USD (at closing)"),
        dcc.Graph(figure=fig_am1),
        html.H1("Amazon Stock Volume"),
        html.P("in total count"),
        dcc.Graph(figure=fig_am2),

        # Nvidia
        html.H1("Nvidia Stock Prices"),
        html.P("in USD (at closing)"),
        dcc.Graph(figure=fig_nv1),
        html.H1("Nvidia Stock Volume"),
        html.P("in total count"),
        dcc.Graph(figure=fig_nv2),

        # Coca Cola
        html.H1("Coca Cola Stock Prices"),
        html.P("in USD (at closing)"),
        dcc.Graph(figure=fig_co1),
        html.H1("Coca Cola Stock Volume"),
        html.P("in total count"),
        dcc.Graph(figure=fig_co2),

        # S&P500 (Volume left out for this security)
        html.H1("S&P 500 Stock Prices"),
        html.P("in USD (at closing)"),
        dcc.Graph(figure=fig_sp1),


        # Gold (Volume left out for this security)
        html.H1("Gold Price"),
        html.P("in USD (at closing)"),
        dcc.Graph(figure=fig_gd1),

    ]
)

if __name__=='__main__':
    app.run_server(debug=True)