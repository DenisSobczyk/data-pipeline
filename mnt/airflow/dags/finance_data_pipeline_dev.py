from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import pandas as pd
import pandas_datareader as pdr
from datetime import datetime, date
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from py.data_ingestion import data_ingestion
from py.preprocess_csv_data import preprocess_csv_data


'''
This file contains all processing / orchestration tasks for the DAG
'''

default_args = {
    "owner": "airflow",
    "retries": 3,
}
# initialize DAG
with DAG("finance_data_pipeline_dev", start_date=datetime(2022, 10 ,31),
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    # data ingestion operation
    data_ingestion = PythonOperator(
        task_id="data_ingestion",
        python_callable=data_ingestion
    )

    # data preprocessing operation
    preprocess_csv_data = PythonOperator(
        task_id="preprocess_csv_data",
        python_callable=preprocess_csv_data
    )

    # store data in hdfs
    save_data_hdfs = BashOperator(
        task_id= "save_data_hdfs",
        bash_command="""
            hdfs dfs -mkdir -p /Finance_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/data /Finance_Data
           
        """
    )
    # create tables in HIVE
    create_table_hive = HiveOperator(
        task_id="create_table_hive",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS amazon_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )

            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE; 
        
                
            CREATE EXTERNAL TABLE IF NOT EXISTS apple_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
             

            CREATE EXTERNAL TABLE IF NOT EXISTS meta_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS alphabet_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS coca_cola_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS walmart_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS caterpillar_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;                

            CREATE EXTERNAL TABLE IF NOT EXISTS nasdaq_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS tesla_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS nvidia_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS disney_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS dow_jones_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS sp500_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;  
                
            CREATE EXTERNAL TABLE IF NOT EXISTS crude_oil_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS gold_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS visa_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS nike_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS goldman_sachs_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS procter_gamble_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS cisco_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS netflix_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS microsoft_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS boeing_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            
            CREATE EXTERNAL TABLE IF NOT EXISTS salesforce_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;    
                
            CREATE EXTERNAL TABLE IF NOT EXISTS home_depot_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;   

            CREATE EXTERNAL TABLE IF NOT EXISTS chevron_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )   
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
                
            CREATE EXTERNAL TABLE IF NOT EXISTS mcdonalds_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;   

            CREATE EXTERNAL TABLE IF NOT EXISTS verizon_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;   

            CREATE EXTERNAL TABLE IF NOT EXISTS walgreens_boots_alliance_Data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
                
            CREATE EXTERNAL TABLE IF NOT EXISTS intel_data(
                `Date` DATE,
                High DOUBLE,
                Low DOUBLE,
                `Open` DOUBLE,
                Close DOUBLE,
                Volume DOUBLE,
                Adj_Close DOUBLE
                )   
                
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE;
            """)

    # process financial security data
    finance_processing = SparkSubmitOperator(
        task_id="finance_processing",
        application= "/opt/airflow/dags/scripts/finance_data_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    # adding dependencies between tasks
    data_ingestion >> preprocess_csv_data >> save_data_hdfs
    save_data_hdfs >> create_table_hive >> finance_processing






