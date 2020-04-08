from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import os
import psycopg2
import boto3
import pandas as pd
import zipfile
import logging
import pymysql
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData
from botocore.exceptions import ClientError


default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['chrischavezdataengineer@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='atp_data06',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)


def get_kaggle():
    url = 'https://www.kaggle.com/c/house-prices-advanced-regression-techniques/data'
    response = requests.get(url)
    with open('test.csv', 'wb') as f:
        f.write(response.content)

def unzip():
    zf = zipfile.ZipFile('/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/house-prices-advanced-regression-techniques.zip')
    df = pd.read_csv(zf.open('Data.csv'), encoding='ISO-8859-1')
    df.to_csv('/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/Data.csv')

T_I = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle competitions download -c house-prices-advanced-regression-techniques -p /Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques',
    dag=dag,
)

T_II = PythonOperator(
    task_id='unzip_api',
    provide_context=False,
    python_callable=unzip,
    dag=dag,
)

T_I >> T_II 