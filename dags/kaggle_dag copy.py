from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import zipfile
import pandas as pd
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
    dag_id = 'atp_data01',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

def get_unrate():
    """
    Gets overall US unemployment rate by month
    """
    url = 'https://www.kaggle.com/c/house-prices-advanced-regression-techniques/data'
    response = requests.get(url)
    with open('test.csv', 'wb') as f:
        f.write(response.content)


# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle competitions download -c house-prices-advanced-regression-techniques',
    dag=dag,
)


t2 = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_connection",
    database="stack_overflow",
    sql="""
    DROP TABLE IF EXISTS public.questions;
    CREATE TABLE public.questions
    (
    title text,
    is_answered boolean,
    link character varying,
    score integer,
    tags text[],
    question_id integer NOT NULL,
    owner_reputation integer
    )
    """,
    )

t3 = PythonOperator(
    task_id="insert_question_to_db", python_callable=insert_question_to_db
    )




















