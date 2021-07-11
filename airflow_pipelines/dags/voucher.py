from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


from datetime import datetime, timedelta

# Following are defaults which can be overridden later on
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

dag = DAG('Helloworld', default_args=default_args)

# Create Table if not present

t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)

t2 = PostgresOperator(
    task_id='task_2',
    bash_command='echo "Hello World from Task 2"',
    dag=dag,
    sql='SELECT * FROM TABLE')

t3 = BashOperator(
    task_id='task_3',
    bash_command='echo "Hello World from Task 3"',
    dag=dag)

t4 = BashOperator(
    task_id='task_4',
    bash_command='echo "Hello World from Task 4"',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t2)
t4.set_upstream(t3)



