from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import airflow.hooks.S3_hook
import datetime as dt
import yfinance as yf
import pandas as pd
import pandasql as ps

## Create Airflow DAG:

default_args = {
    "start_date" : dt.datetime(2021, 8, 15),     
    "retries" : 2,                                  # Retry twice
    "retry_delay" : dt.timedelta(minutes=5),        # Retry with a 5-minute interval
}

dag = DAG(
    dag_id = "marketvol2",
    default_args = default_args,
    description = "A simple DAG",      
    schedule_interval = "0 18 * * 1-5",             # Run daily, from Monday to Friday, at 06:00 PM
    catchup=False
)


## Initialize a temporary directory for data download

# Templated bash command to parameterize directory naming using the execution date
make_dir = """
    mkdir -p /usr/local/airflow/21.7_Airflow_DAG/data/temp/{{ yesterday_ds }}
"""

# Bash Operator for temporary directory creation
temp_directory = BashOperator(task_id="temp_directory",
                        bash_command = make_dir,
                        dag = dag)


## Download source data from Yahoo Finance and save it as CSV files in temp. directory

def data_download(ticker, **kwargs):
    '''Download daily stock prices based on company's ticker at 1 minute of granularity.
            
    Args:
        ticker (str): Stock symbol
    
    Returns:
        N/A    
  
    '''

    start_date = dt.date.today() - dt.timedelta(days=1)        # Needs to be a weekday
    end_date = start_date + dt.timedelta(days=1)
    tsla_df = yf.download(ticker, start = start_date, end = end_date, interval = "1m")

    date = kwargs['yesterday_ds']
    tsla_df.to_csv("/usr/local/airflow/21.7_Airflow_DAG/data/temp/" + date + "/" + ticker + ".csv", header=False)

# Python Operators to call data_download function per listing
aapl_download = PythonOperator(task_id="aapl_download",
                        provide_context = True,
                        python_callable = data_download,
                        op_kwargs = {"ticker" : "AAPL"},
                        dag = dag)

tsla_download = PythonOperator(task_id="tsla_download",
                        provide_context = True,
                        python_callable = data_download,
                        op_kwargs = {"ticker" : "TSLA"},
                        dag = dag)


## Create directory per day in local file system to move downloaded files

downloaded_data_directory = BashOperator(task_id="downloaded_data_directory",
                        bash_command = "mkdir -p /usr/local/airflow/21.7_Airflow_DAG/data/query/{{ yesterday_ds }}",
                        dag = dag)

## Move downloaded files from temp. directory to local file system

# Templated bash command to parameterize paths of files to move based on execution date
mv = """
    mv /usr/local/airflow/21.7_Airflow_DAG/data/temp/{{ yesterday_ds }}/{{ params.filename }}  /usr/local/airflow/21.7_Airflow_DAG/data/query/{{ yesterday_ds }}
"""
# Bash Operators to transfer files from temporary directory to query directory
aapl_transfer = BashOperator(task_id="aapl_transfer",
                        bash_command = mv,
                        params = {"filename" : "AAPL.csv"},
                        dag = dag)

tsla_transfer = BashOperator(task_id="tsla_transfer",
                        bash_command = mv,
                        params = {"filename" : "TSLA.csv"},
                        dag = dag)


# PythonOperator to run a query on both data files

def data_query(ticker1, ticker2, **kwargs):
    '''Download daily stock prices based on company's ticker at 1 minute of granularity.
            
    Args:
        ticker (str): Stock symbol
    
    Returns:
        N/A    
  
    '''
    header_list = ["date_time", "open", "high", "low", "close", "adj_close", "volume"]

    ticker1_df = pd.read_csv(f"/usr/local/airflow/21.7_Airflow_DAG/data/query/" + kwargs['yesterday_ds'] + "/" + ticker1 + ".csv", names = header_list)
    ticker2_df = pd.read_csv(f"/usr/local/airflow/21.7_Airflow_DAG/data/query/" + kwargs['yesterday_ds'] + "/" + ticker2 + ".csv", names = header_list)

    q1 = """SELECT max(high) as highest FROM ticker1_df"""
    q2 = """SELECT max(high) as highest FROM ticker2_df"""

    r1 = ps.sqldf(q1, locals())
    r2 = ps.sqldf(q2, locals())

    if r1['highest'].iloc[0] > r2['highest'].iloc[0]:
        result = ticker1 + " listing: " + str(r1['highest'].iloc[0]) + " was higher than " + ticker2 + " listing: " + str(r2['highest'].iloc[0]) + " on " + kwargs['yesterday_ds']
    else:
        result = ticker2 + " listing: " + str(r2['highest'].iloc[0]) + " was higher than " + ticker1 + " listing: " + str(r1['highest'].iloc[0]) + " on " + kwargs['yesterday_ds']

    text_file = open("/usr/local/airflow/21.7_Airflow_DAG/data/query/" + kwargs['yesterday_ds'] + "/results.txt", "w")
    text_file.write(result)
    text_file.close()
    

files_query = PythonOperator(task_id="files_query",
                        provide_context = True,
                        python_callable = data_query,
                        op_kwargs = {"ticker1" : "AAPL", "ticker2" : "TSLA"},
                        dag = dag)


# Upload query results to S3 bucket using S3 hook

def upload_S3_hook(bucket_name, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook("my_S3")
    file = "/usr/local/airflow/21.7_Airflow_DAG/data/query/" + kwargs['yesterday_ds'] + "/results.txt"
    key = "21.7_Airflow_DAG/" + kwargs['yesterday_ds'] + "/results.csv"
    hook.load_file(file, key, bucket_name)

upload_results_S3 = PythonOperator(
    task_id = "upload_results_S3",
    provide_context = True,
    python_callable = upload_S3_hook,
    op_kwargs={ "bucket_name" : "aaa-raw-data"},
    dag = dag)

# Job Dependencies
temp_directory >> aapl_download >> downloaded_data_directory >> aapl_transfer >> files_query >> upload_results_S3
temp_directory >> tsla_download >> downloaded_data_directory >> tsla_transfer >> files_query >> upload_results_S3