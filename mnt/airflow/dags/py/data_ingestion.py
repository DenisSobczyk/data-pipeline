from datetime import date
import pandas_datareader as pdr

def data_ingestion():
    ticker_list = ["AAPL", "AMZN", "META", "GOOG", "KO", "WMT", "CAT", "^IXIC", "TSLA", "NVDA", \
                   "DIS", "DOW", "^GSPC", "CL=F", "GC=F", "V", "NKE", "GS", "PG", "CSCO", \
                   "NFLX", "MSFT", "BA", "CRM", "HD", "CVX", "MCD", "VZ", "WBA", "INTC"]
    start_date = "2000-01-01"
    end_date = date.today()
    for i in ticker_list:
        data = pdr.get_data_yahoo(i, start_date, end_date)
        data.to_csv('/opt/airflow/dags/data/{}_data_yahoo.csv'.format(i))