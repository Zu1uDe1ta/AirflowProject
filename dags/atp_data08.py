






def upload_file(file_name, bucket, object_name=None):
    """
    upload file to an S3 bucket
    :param file_name: file to be upkoaded
    :param bucket:
    :param object_name:
    :return:
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def insert_data():
    upload_file('/Users/psehgal/atp_test/Data.csv', 'preeti.first.boto.s3.bucket', 'atpdata')


def insert_sql():
    df = pd.read_csv('/Users/psehgal/atp_test/Data.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/ATP_tennis')
    df.to_sql(name='atprawdata', con=engine, index=False, if_exists='replace')


def remove_files():
    os.remove('/Users/psehgal/atp_test/atp-tour-20002016.zip')
    os.remove('/Users/psehgal/atp_test/Data.csv')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['preetisehgal2001@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    dag_id='atp_dag_etl_final',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle datasets download -d jordangoblet/atp-tour-20002016 -p /Users/psehgal/atp_test',
    dag=dag,
)

t2 = PythonOperator(
    task_id='unzip_api',
    provide_context=False,
    python_callable=unzip,
    dag=dag,
)

t3 = PythonOperator(
        task_id='move_file_to_AWS_S3',
        provide_context=False,
        python_callable=insert_data,
        dag=dag
)

t4 = PythonOperator(
        task_id='move_data_from_file_to_SQL',
        provide_context=False,
        python_callable=insert_sql,
        dag=dag,
)

t5 = PythonOperator(
        task_id='remove_file',
        provide_context=False,
        python_callable=remove_files,
        dag=dag,
)

t1 >> t2 >> [t3, t4] >> t5