import pandas as pd


def preprocess_csv_data():
    ticker_list = ["AAPL", "AMZN", "META", "GOOG", "KO", "WMT", "CAT", "^IXIC", "TSLA", "NVDA", \
                   "DIS", "DOW", "^GSPC", "CL=F", "GC=F", "V", "NKE", "GS", "PG", "CSCO", \
                   "NFLX", "MSFT", "BA", "CRM", "HD", "CVX", "MCD", "VZ", "WBA", "INTC"]
    for i in ticker_list:
        df = pd.read_csv('/opt/airflow/dags/data/{}_data_yahoo.csv'.format(i))
        df.rename({'Adj Close': 'Adj_Close'}, axis=1, inplace=True)
        df.index = pd.to_datetime((df.index))
        df.to_csv('opt/airflow/dags/data/{}_data_yahoo.csv'.format(i), index=False)