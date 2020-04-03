from datetime import datetime, timedelta

from airflow import DAG



default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 3),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
}

with DAG("stack_overflow_questions", default_args=default_args) as dag:

    Task_I = PostgresOperator(
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

    Task_II = PythonOperator(
        task_id="insert_question_to_db", python_callable=insert_question_to_db
    )
