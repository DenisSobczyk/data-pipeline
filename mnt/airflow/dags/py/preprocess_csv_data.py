import pandas as pd

'''
This file will be loaded and processed in the file where 
the DAG is stored ('finance_data_processing_dev')
'''

def preprocess_csv_data():
    # list of tickers that must be considered to get queried from Yahoo! Finance API
    ticker_list = ["AAPL", "AMZN", "META", "GOOG", "KO", "WMT", "CAT", "^IXIC", "TSLA", "NVDA", \
                   "DIS", "DOW", "^GSPC", "CL=F", "GC=F", "V", "NKE", "GS", "PG", "CSCO", \
                   "NFLX", "MSFT", "BA", "CRM", "HD", "CVX", "MCD", "VZ", "WBA", "INTC"]

    # change the column name "Adj Close" to "Adj_Close"
    for i in ticker_list:
        df = pd.read_csv('/opt/airflow/dags/data/{}_data_yahoo.csv'.format(i))
        df.rename({'Adj Close': 'Adj_Close'}, axis=1, inplace=True)
        df.to_csv('/opt/airflow/dags/data/{}_data_yahoo.csv'.format(i), index=False)