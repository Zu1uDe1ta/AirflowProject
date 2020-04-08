from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import os
import psycopg2
import pandas as pd
import zipfile
import logging
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData



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
    dag_id='atp_data07',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)


def get_kaggle():
    url = 'https://www.kaggle.com/c/house-prices-advanced-regression-techniques/data'
    response = requests.get(url)
    with open('sample_submission.csv', 'wb') as f:
        f.write(response.content)

def unzip():
    zf = zipfile.ZipFile('/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/house-prices-advanced-regression-techniques.zip')
    df = pd.read_csv(zf.open('sample_submission.csv'), encoding='ISO-8859-1')
    df.to_csv('/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/sample_submission.csv')

# test connection to postgres
def connect_pst():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")

# created sample_submission table
def create_tbl_sample():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE sample(
        Id varchar(255),
        SalePrice numeric
    )
    """)
    conn.commit()

def insert_csv_sample():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/sample_submission.csv", 'r') as f:
        next(f)
        cur.copy_from(f, 'sample', sep=',')
        conn.commit()

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

T_III = PythonOperator(
    task_id='connect_pst',
    provide_context=False,
    python_callable=connect_pst,
    dag=dag,
)

T_IV = PythonOperator(
    task_id='create_tbl_sample',
    provide_context=False,
    python_callable=create_tbl_sample,
    dag=dag,
)

T_V = PythonOperator(
    task_id='insert_csv_sample',
    provide_context=False,
    python_callable=insert_csv_sample,
    dag=dag,
)

T_I >> T_II >> T_III >> T_IV >> T_V