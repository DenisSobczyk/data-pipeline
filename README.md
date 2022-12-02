Implementation

Before the procedure can be conducted one needs to check if the needed software is installed. Here, a Python-compatible IDE (e.g., PyCharm or VisualStudio) must be installed as well as the Docker software including Docker-Compose. 
Thereafter download/clone the software from the GitHub repository: [enter repository]
To start the container navigate in the command line to the path where the cloned/downloaded data has been stored and enter "chmod +x start.sh" and "./start.sh" to start the container environment. 
Moreover, the Airflow connections need to be set by adding the Hive connection:
Conn id = hive_conn
Conn Type= Hive Server 2 Theft
Host = hive-server
Login = hive
Password = hive
Port = 10000

Create also the Spark connection as follows:
Conn id = spark_conn
Conn Type=Spark
Host = spark://spark-master
Port = 7077
After the connections have been set, the data pipeline is ready to be started by clicking on the Dag "finance_data_pipeline_dev" in the airflow interface trigger to run the data pipeline. Eventually, the data should be finally processed to the HIVE tables. The user interface can be found on localhost: 32762
