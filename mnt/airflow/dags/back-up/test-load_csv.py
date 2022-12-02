from datetime import date
import pandas_datareader as pdr

def load_csv():
    ticker_list = ["AAPL", "AMZN", "META", "GOOG", "KO"]
    start_date = "1990-01-01"
    end_date = date.today()
    for i in ticker_list:
        data = pdr.get_data_yahoo(i, start_date, end_date)
        data.to_csv('{}_data_yahoo.csv'.format(i))

load_csv()

