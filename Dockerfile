FROM puckel/docker-airflow:1.10.9
RUN pip install requests
RUN pip install pandas
RUN pip install boto3 --upgrade
RUN  pip install 'apache-airflow[s3]'
RUN pip install yfinance
RUN pip install pandasql

# User root
# RUN chown -R airflow: ${AIRFLOW_USER_HOME}

# USER airflow
