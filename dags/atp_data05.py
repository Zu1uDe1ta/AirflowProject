from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['chrischavezdataengineer@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=6),
}

dag = DAG(
    dag_id='atp_data05',
    default_args=default_args,
    description='house prices from kaggle',
    schedule_interval=timedelta(days=1),
)
# get stock data
get_data = BashOperator(
    task_id='get_kaggle_api',
    bash_command='kaggle competitions download -c house-prices-advanced-regression-techniques',
    dag=dag,
)


# test connection to postgres
def connect_pst():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")


# connect and create table
def create_tbl_train():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE train(
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
    )
    """)
    conn.commit()


# connect and insert
def insert_csv_train():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/train.csv", 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'train', sep=',')
        conn.commit()


# created test table
def create_tbl_test():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE test(
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
    )
    """)
    conn.commit()


# insert stock csv data
def insert_csv_test():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/cchavez/dev/AirflowProject/house-prices-advanced-regression-techniques/train.csv/test.csv", 'r') as f:
        next(f)
        cur.copy_from(f, 'test', sep=',')
        conn.commit()

def insert_raw_data():
    engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost/testDB')
    df = pd.read_csv("/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new/USvideos.csv",
                     delimiter=',')
    with engine.connect() as conn, conn.begin():
        df.to_sql('raw_data', conn, if_exists='replace')

t1 = PythonOperator(
    task_id='connect_pst',
    provide_context=False,
    python_callable=connect_pst,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_tbl_train',
    provide_context=False,
    python_callable=create_tbl_train,
    dag=dag,
)
t3 = PythonOperator(
    task_id='insert_train',
    provide_context=False,
    python_callable=insert_csv_train,
    dag=dag,
)

t4 = PythonOperator(
    task_id='create_tbl_test',
    provide_context=False,
    python_callable=create_tbl_test,
    dag=dag,
)
t5 = PythonOperator(
    task_id='insert_csv_test',
    provide_context=False,
    python_callable=insert_csv_test,
    dag=dag,
)

t1
t2 >> t3 
t4 >> t5 
# merge the two table columns
# SELECT *
# FROM
# sec, stock_dummy
# WHERE
# sec.date = stock_dummy.date;
"""
#### didn't work
CREATE TABLE stock_sec
AS
SELECT open, high, low, close, adj_close, volume,date
FROM stock_dummy
UNION
SELECT secformname, description, date
FROM sec;
"""
# after this merge need to split the tables in to years