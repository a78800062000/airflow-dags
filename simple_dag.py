from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def my_print_function():
    """
    打印一條簡單的消息
    """
    print("Hello from Airflow!")

# 定義DAG參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定義DAG
dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='一個簡單的教學 DAG',
    schedule_interval=timedelta(days=1),
)

# 定義任務
t1 = PythonOperator(
    task_id='print_hello',
    python_callable=my_print_function,
    dag=dag,
)

# 設置任務流
t1
