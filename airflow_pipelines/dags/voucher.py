from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from util.voucher_table_sql import VOUCHER_SQL
from util.data_cleansing import transform_load


default_args = {
    'owner': 'Roshan',
    'description': 'A DAG to cleanse and populate a DB with historic voucher data',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 10),
    'email': ['rossi.zameer@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

dag = DAG('Voucher_Select', default_args=default_args)

# Create Table if not present

create_db = PostgresOperator(
    task_id='create_db',
    postgres_conn_id='postgres_default',
    dag=dag,
    sql=VOUCHER_SQL)


# Download Parquet Dataset

extract_data = BashOperator(
    task_id='extract',
    bash_command='wget https://dh-data-chef-hiring-test.s3.eu-central-1.amazonaws.com/data-eng/voucher-selector/data.parquet.gzip -P /usr/local/airflow',
    dag=dag)


# Clean the data and load it into a DB

data_cleansing = PythonOperator(
    python_callable=transform_load,
    task_id='clean_and_load',
    op_kwargs={'parquet_file': '/usr/local/airflow/data.parquet.gzip'},
    dag=dag)


# Dummy Operator
dummy = DummyOperator(
    task_id='Finish',
    dag=dag)


create_db >> extract_data >> data_cleansing >> dummy
