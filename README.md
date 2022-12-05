# Data Engineering Project: Create a Stock Data Pipeline

## Project Description
The basic idea of this data pipeline project is to use the massive amounts of finance security data that is provided by the world wide web. Within the framework of this project 30 securities, which are mainly big companies or indices in the United States, are ingested and stored to a distributed file system by leveraging the batch processing ability of Apache Spark to process data into HIVE tables. Therein, data from the year 2000 until the day of execution are considered (attention: not all securities have a data history for this period). Eventually, 5 securities will be visualized by the application of Dash. The data pipeline will be orchestrated by Apache Airflow. Moreover, the execution of the sub-processes will be conducted on Docker containers.

The intention of this project is to move the data from API to a disturbed file system automatically so more time can be saved on Data Analytics tasks since data is provided without any significant effort.

## Conceptual Framework

The picture below illustrates the conceptual framework of this project as a flow chart. As one can note here, the data pipeline orechstration is handled by Apache Airflow. Moreover, the whole environment of the data pipline is built in Docker containers.
Consequently, the data pipeline contains the following main steps:

#### Backend (blue boxes)
1. Data ingestion from the Yahoo Finance API for the 30 security values  | _Apache Airflow task_
2. Execution of data preprocessing using Python  | _Apache Airflow task_
3. Storing data into HDFS filesystem  | _Apache Airflow task_
4. Creating tables into the HIVE file system  | _Apache Airflow task_
5. Processing data into the previously created HIVE tables using Apache Spark  | _Apache Airflow task_
6. After this step has been executed, the processe | _Apache Airflow task_

#### Frontend (yellow boxes)
7. Open Hue to query the 30 created data tables | _localhost:32762, user=admin, password=admin_
8. Enter the dash app to see visualization of securities | _localhost:5001_

For the visualization, the _Close_ and the _Volume_ attribute of the following securities have been considered: 

Amazon, Nvidia, Coca Cola, S&P500, and Gold commodity.

![Conceptual Framework](https://github.com/DenisSobczyk/data-pipeline/blob/main/Architecture_ProcessChart_20221205-1.png)

## Technical prerequisites & Execution Manual
This project requires to have Docker and Docker-Compose installed on your computer. Moreover, a Python IDE as well a textfile editor of your choice could also be helpful. For the Docker hardware resources 4 gigabyte RAM have been used to run the environment.

In order to start the application, please execute the following steps in the according sequence:

1. Clone this repository
2. Within your terminal, navigate to the folder where this project has been stored 
3. Execute the command: chmod +x start.sh 
4. Execute the command: ./start.sh
5. Enter Apache Airflow on localhost:8080 (username=airflow, password=airflow)
6. Set the HIVE connection:

		Conn id = _hive_conn_
		Conn Type = _Hive Server 2 Theft_
		Host = _hive-server_
		Login = _hive_
		Password = _hive_
		Port = 10000 
7. Set the Spark connection:

		Conn id = _spark_conn_
		Conn Type = _Spark_
		Host = _spark://spark-master_
		Port = 7077
