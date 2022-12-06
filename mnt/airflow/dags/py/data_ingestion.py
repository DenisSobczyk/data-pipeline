from datetime import date
import pandas_datareader as pdr

'''
This file will be loaded and processed in the file where 
the DAG is stored ('finance_data_processing_dev')
'''

# tickers that need to be queried from Yahoo! Finance API
def data_ingestion():
    ticker_list = ["AAPL", "AMZN", "META", "GOOG", "KO", "WMT", "CAT", "^IXIC", "TSLA", "NVDA", \
                   "DIS", "DOW", "^GSPC", "CL=F", "GC=F", "V", "NKE", "GS", "PG", "CSCO", \
                   "NFLX", "MSFT", "BA", "CRM", "HD", "CVX", "MCD", "VZ", "WBA", "INTC"]

    # set the time frame
    start_date = "2000-01-01"
    end_date = date.today()

    # for-loop to iterate through every ticker and save the data as CSV file
    for i in ticker_list:
        data = pdr.get_data_yahoo(i, start_date, end_date)
        data.to_csv('/opt/airflow/dags/data/{}_data_yahoo.csv'.format(i))