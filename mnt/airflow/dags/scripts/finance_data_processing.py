from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Create Spark Session

spark = SparkSession \
    .builder \
    .appName("Finance Data") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


# create dictionary to map tickers with data table names
dict_ = {'AAPL': 'apple', 'AMZN': 'amazon', 'BA':'boeing', 'CAT': 'caterpillar',\
         'CL=F':'crude_oil', 'CRM': 'salesforce', 'CSCO':'cisco', 'CVX':'chevron', \
         'DIS':'disney', 'DOW':'dow_jones', 'GC=F':'gold', 'GOOG':'alphabet', \
         'GS': 'goldman_sachs', 'HD':'home_depot', 'INTC':'intel', 'KO':'coca_cola',\
         'MCD':'mcdonalds', 'META': 'meta', 'MSFT':'microsoft', 'NFLX': 'netflix',
         'NKE':'nike', 'NVDA': 'nvidia', 'PG':'procter_gamble', 'TSLA':'tesla', \
         'VZ':'verizon', 'V':'visa', 'WBA':'walgreens_boots_alliance', \
         'WMT': 'walmart', '^GSPC':'sp500', '^IXIC':'nasdaq'}

#dict_1 = {'AAPL': 'apple' }

for i in dict_:

    try:
        # Read the file from the HDFS
        df = spark.read.csv('hdfs://namenode:9000/Finance_Data/data/{}_data_yahoo.csv'.format(i), header=True)
        # Load the columns and create dataframe
        data = df.select('Date','High', 'Low', 'Open', 'Close', 'Volume', 'Adj_Close')
        # Export the dataframe into the Hive table forex_rates
        data.write.mode("append").insertInto("{}_data".format(dict_[i]))
    except:
        print(i, " includes a mistake")