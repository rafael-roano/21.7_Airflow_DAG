# Mini Project - DAG Scheduling on Airflow

This is the implementation of a Data Pipeline using Airflow toguether with Docker. Data pipeline extracts online stock market data from Yahoo Finance and delivers simple analytical results. Yahoo Finance provides intra-day market price details.

## Description

The data pipeline extracts stock price information (in one-minute intervals) from Yahoo Finance, on a daily basis, for Apple and Tesla stocks. THe downlaoded files are stored temporarly in local file system, transfered to a specific location to query them, and once results from query are obtained, these are uploaded to S3 bucket. 

* Data Source: Yahoo Finance via yfinance library
* Granularity: Daily in one-minute intervals
* Stocks of Interest: AAPL and TSLA
* Workfload schdule: Daily, from Monday to Friday at 06:00 PM
* Airflow Executor: Celery

Pipeline consists of tasks:

1. temp_directory: Bash Operator to create temporary directory per day in local file system

2. aapl_download and tsla_download: Python Operators to call data_download function per listing

3. downloaded_data_directory: Bash Operator to create directory per day in local file system to move downloaded files to query directory

4. aapl_transfer and tsla_transfer: Bash Operators to transfer files from temporary directory to query directory

5. files_query: PythonOperator to run a query on both data files

6. upload_results_S3: PythonOperator to upload query results to S3 bucket using S3 hook


## Getting Started

# Prerequisites

* Install [Docker][1]
* Install [Docker Compose][2]

[1]: https://www.docker.com/
[2]: https://docs.docker.com/compose/install/


**Files**

1. Download the following files from github[3]:

* marketvol2.py
* airflow.cfg
* Dockerfile
* docker-compose-rr.yml 

2. Save docker-compose-rr.yml on {AIRFLOW_HOME} 
3. Save airflow.cfg on config directory
4. Save marketvol2.py on DAGS directory
5. Save Dockerfile on dockerfiles directory

[3]: https://github.com/rafael-roano/21.7_Airflow_DAG.git


**Usage**

1. Create /home/airflow/21.7_Airflow_DAG folder (volume to persist information from container to local file system)

```
mkdir /home/airflow/21.7_Airflow_DAG
```

2. Build image within dockerfiles directory

```
docker build -t puckel/docker-airflow-rr:latest
```

3. Create and start container 

```
docker-compose -f docker-compose-rr.yml up -d
```

4. Check http://localhost:8080/ for webserver UI
5. Check http://localhost:5555/ for Flower UI


## Authors

Rafael Roano

## Version History

* 0.1
    * Initial Release