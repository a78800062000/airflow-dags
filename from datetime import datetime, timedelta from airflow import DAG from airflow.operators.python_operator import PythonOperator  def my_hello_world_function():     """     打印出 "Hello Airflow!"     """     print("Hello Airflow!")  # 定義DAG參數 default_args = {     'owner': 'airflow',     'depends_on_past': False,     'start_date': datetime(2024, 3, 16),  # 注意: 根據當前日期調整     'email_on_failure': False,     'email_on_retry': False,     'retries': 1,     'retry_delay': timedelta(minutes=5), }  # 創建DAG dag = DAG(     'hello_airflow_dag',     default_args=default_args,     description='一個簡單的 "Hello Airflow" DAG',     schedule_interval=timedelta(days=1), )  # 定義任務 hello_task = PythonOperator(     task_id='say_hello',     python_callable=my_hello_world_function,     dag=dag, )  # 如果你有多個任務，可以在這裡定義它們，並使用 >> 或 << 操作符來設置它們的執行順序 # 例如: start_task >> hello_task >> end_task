from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def my_hello_world_function():
    """
    "Hello Airflow!"
    """
    print("Hello Airflow!")

# 定義DAG參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 創建DAG
dag = DAG(
    'hello_airflow_dag',
    default_args=default_args,
    description='"Hello Airflow" DAG',
    schedule_interval=timedelta(days=1),
)

# 定義任務
hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=my_hello_world_function,
    dag=dag,
)
