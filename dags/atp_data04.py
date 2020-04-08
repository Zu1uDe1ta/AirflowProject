from airflow.operators.postgres_operator import PostgresOperatorimport uuid
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from datetime import datetime


dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime.now(),
    'schedule_interval': None
}


with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE new_table(
            custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
            );''',
    )

    insert_row = PostgresOperator(
        task_id='insert_row',
        sql='INSERT INTO new_table VALUES(%s, %s, %s)',
        trigger_rule=TriggerRule.ALL_DONE,
        parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
    )

    create_table >> insert_row

default_args = {
    'owner': 'CChavez',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['chrischavezdataengineer@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG(
    dag_id='atp_data04',
    default_args=default_args,
    description='create and insert',
    schedule_interval=timedelta(days=1),
)


def connect_pst():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")

def create_test_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE house_prices.test (
        MSSubClass numeric, 
        MSZoning varchar(255),
        LotFrontage numeric,
        LotArea numeric,
        Street varchar(255), 
        Alley varchar(255),
        LotShape varchar(255),
        LandContour varchar(255),
        Utilities varchar(255),
        LotConfig varchar(255),
        LandSlope varchar(255),
        Neighborhood varchar(255),
        Condition1 varchar(255),
        Condition2 varchar(255),
        BldgType varchar(255),
        HouseStyle varchar(255),
        OverallQual numeric, 
        OverallCond  numeric, 
        YearBuilt  numeric, 
        YearRemodAdd  numeric, 
        RoofStyle varchar(255),
        RoofMatl varchar(255),
        Exterior1st varchar(255),
        Exterior2nd varchar(255),
        MasVnrType varchar(255),
        MasVnrArea  numeric, 
        ExterQual varchar(255),
        ExterCond varchar(255),
        Foundation  varchar(255),
        BsmtQual varchar(255),
        BsmtCond  varchar(255),
        BsmtExposure varchar(255),
        BsmtFinType1 varchar(255),
        BsmtFinSF1 numeric, 
        BsmtFinType2 varchar(255),
        BsmtFinSF2 numeric, 
        BsmtUnfSF numeric,  
        TotalBsmtSF numeric,  
        Heating varchar(255),
        HeatingQC varchar(255),
        CentralAir varchar(255),
        Electrical varchar(255),
        1stFlrSF numeric, 
        2ndFlrSF  numeric, 
        LowQualFinSF  numeric, 
        GrLivArea numeric, 
        BsmtFullBath numeric, 
        BsmtHalfBath numeric, 
        FullBath numeric, 
        HalfBath  numeric, 
        BedroomAbvGr numeric, 
        KitchenAbvGr numeric, 
        KitchenQual varchar(255),
        TotRmsAbvGrd numeric, 
        Functional varchar(255),
        Fireplaces numeric, 
        FireplaceQu varchar(255),
        GarageType varchar(255),
        GarageYrBlt  numeric, 
        GarageFinish varchar(255),
        GarageCars  numeric, 
        GarageArea  numeric, 
        GarageQual varchar(255),
        GarageCond varchar(255),
        PavedDrive varchar(255),
        WoodDeckSF  numeric, 
        OpenPorchSF  numeric, 
        EnclosedPorch numeric, 
        3SsnPorch  numeric, 
        ScreenPorch  numeric, 
        PoolArea numeric, 
        PoolQC varchar(255),
        Fence varchar(255),
        MiscFeature varchar(255),
        MiscVal numeric, 
        MoSold numeric, 
        YrSold numeric, 
        SaleType varchar(255),
        SaleCondition varchar(255),
        SalePrice numeric
);
    )
    """)

    conn.commit()

def create_train_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE house_prices.train (
        MSSubClass numeric, 
        MSZoning varchar(255),
        LotFrontage numeric,
        LotArea numeric,
        Street varchar(255), 
        Alley varchar(255),
        LotShape varchar(255),
        LandContour varchar(255),
        Utilities varchar(255),
        LotConfig varchar(255),
        LandSlope varchar(255),
        Neighborhood varchar(255),
        Condition1 varchar(255),
        Condition2 varchar(255),
        BldgType varchar(255),
        HouseStyle varchar(255),
        OverallQual numeric, 
        OverallCond  numeric, 
        YearBuilt  numeric, 
        YearRemodAdd  numeric, 
        RoofStyle varchar(255),
        RoofMatl varchar(255),
        Exterior1st varchar(255),
        Exterior2nd varchar(255),
        MasVnrType varchar(255),
        MasVnrArea  numeric, 
        ExterQual varchar(255),
        ExterCond varchar(255),
        Foundation  varchar(255),
        BsmtQual varchar(255),
        BsmtCond  varchar(255),
        BsmtExposure varchar(255),
        BsmtFinType1 varchar(255),
        BsmtFinSF1 numeric, 
        BsmtFinType2 varchar(255),
        BsmtFinSF2 numeric, 
        BsmtUnfSF numeric,  
        TotalBsmtSF numeric,  
        Heating varchar(255),
        HeatingQC varchar(255),
        CentralAir varchar(255),
        Electrical varchar(255),
        1stFlrSF numeric, 
        2ndFlrSF  numeric, 
        LowQualFinSF  numeric, 
        GrLivArea numeric, 
        BsmtFullBath numeric, 
        BsmtHalfBath numeric, 
        FullBath numeric, 
        HalfBath  numeric, 
        BedroomAbvGr numeric, 
        KitchenAbvGr numeric, 
        KitchenQual varchar(255),
        TotRmsAbvGrd numeric, 
        Functional varchar(255),
        Fireplaces numeric, 
        FireplaceQu varchar(255),
        GarageType varchar(255),
        GarageYrBlt  numeric, 
        GarageFinish varchar(255),
        GarageCars  numeric, 
        GarageArea  numeric, 
        GarageQual varchar(255),
        GarageCond varchar(255),
        PavedDrive varchar(255),
        WoodDeckSF  numeric, 
        OpenPorchSF  numeric, 
        EnclosedPorch numeric, 
        3SsnPorch  numeric, 
        ScreenPorch  numeric, 
        PoolArea numeric, 
        PoolQC varchar(255),
        Fence varchar(255),
        MiscFeature varchar(255),
        MiscVal numeric, 
        MoSold numeric, 
        YrSold numeric, 
        SaleType varchar(255),
        SaleCondition varchar(255)
);
    )
    """)

    conn.commit()

def create_submission_table():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE oura_readiness (
    
    /*Readiness has only one row per sleep event*/
    
    CONSTRAINT readiness_id PRIMARY KEY (summary_date, period_id),
    period_id       SMALLINT,
    score_activity_balance  SMALLINT,
    score_previous_day  SMALLINT,
    score_previous_night    SMALLINT,
    score_recovery_index    SMALLINT,
    score_resting_hr    SMALLINT,
    score_sleep_balance SMALLINT,
    score_temperature   SMALLINT,
    summary_date        DATE,
    score           SMALLINT
);
    )

def insert_csv():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/jthompson/dev/airflow_home/Oura_____.csv", 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'sec', sep=',')
        conn.commit()

t1 = PythonOperator(
    task_id='connect_pst',
    provide_context=False,
    python_callable=connect_pst,
    dag=dag,
)

t3 = PythonOperator(
    task_id='create_sleep_table',
    provide_context=False,
    python_callable=create_sleep_table,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_activity_table',
    provide_context=False,
    python_callable=create_activity_table,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_readiness_table',
    provide_context=False,
    python_callable=create_readiness_table(),
    dag=dag,
)
t3 = PythonOperator(
    task_id='insert_csv',
    provide_context=False,
    python_callable=insert_csv,
    dag=dag,
)

"""
# change date type for both TABLE
def clean_data_type_sec():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
    alter table sec
    alter column "date" type date using ("date"::text::date)""")
    conn.commit()


def clean_data_type_stock():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
    alter table stock
    alter column "date" type date using ("date"::text::date)""")
    conn.commit()
"""

t6 = PythonOperator(
    task_id='clean_data_type_sec',
    provide_context=False,
    python_callable=clean_data_type_sec,
    dag=dag,
)
t7 = PythonOperator(
    task_id='clean_data_type_stock',
    provide_context=False,
    python_callable=insert_csv_Stock,
    dag=dag,
)
