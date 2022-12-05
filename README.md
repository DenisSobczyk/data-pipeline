# Data Engineering Project: Create a Stock Data Pipeline

## Project Description
The basic idea of this data pipeline project is to use the massive amounts of finance security data that is provided by the world wide web. Within the framework of this project 30 securities, which are mainly big companies or indices in the United States, are ingested and stored to a distributed file system by leveraging the batch processing ability of Apache Spark to process data into HIVE tables. Therein, data from the year 2000 until the day of execution are considered (attention: not all securities have a data history for this period). Eventually, 5 securities will be visualized by the application of Dash. The data pipeline will be orchestrated by Apache Airflow. Moreover, the execution of the sub-processes will be conducted on Docker containers.

The intention of this project is to move the data from API to a disturbed file system automatically so more time can be saved on Data Analytics tasks since data is provided without any significant effort.

## Conceptual Framework

!Architecture_ProcessChart_20221205.pdf
