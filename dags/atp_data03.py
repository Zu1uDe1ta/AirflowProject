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
    dag_id='atp_data03',
    default_args=default_args,
    description='ATP data from kaggle API',
    schedule_interval=timedelta(days=1),
)


def get_kaggle():
    url = 'https://www.kaggle.com/c/house-prices-advanced-regression-techniques/data'
    response = requests.get(url)
    with open('test.csv', 'wb') as f:
        f.write(response.content)


T_I = BashOperator(
    task_id='run_kaggle_api',
    bash_command='kaggle competitions download -c house-prices-advanced-regression-techniques',
    dag=dag,
)

T_II = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_connection',
    database="house_prices",
    sql='''
	DROP TABLE IF EXISTS house_prices.test;
    CREATE TABLE house_prices.test
    (
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
        SalePrice numeric; ''',
    dag=dag,
)

path = os.path.join(os.path.dirname(__file__),'../test.csv')

T_III = PostgresOperator(
    task_id = 'import_to_postgres',
    postgres_conn_id = 'postgres_connection',
    sql = f"DELETE FROM test.csv; COPY test.csv FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag = dag,
    )



T_I >> T_II >> T_III