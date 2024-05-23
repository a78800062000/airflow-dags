from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_page_index_and_date_mapping_table',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def find_latest_url():
    """找到最新的 MacShop URL，使用重试策略和超时设置。"""
    session = requests.Session()
    retries = Retry(total=5,  # 总尝试次数
                    backoff_factor=1,  # 重试等待时间间隔因子
                    status_forcelist=[429, 500, 502, 503, 504])  # 指定哪些响应码进行重试
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        response = session.get('https://www.ptt.cc/bbs/MacShop/index.html', timeout=10)  # 10秒超时
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        prev_link = soup.find('a', string='‹ 上頁')
        if prev_link:
            prev_page_url = prev_link['href']
            prev_page_number = int(prev_page_url.split('index')[1].split('.html')[0])
            latest_page_number = prev_page_number + 1
            latest_url = f'https://www.ptt.cc/bbs/MacShop/index{latest_page_number}.html'
            return latest_url
        else:
            raise ValueError("未能找到上一页的链接")
    except Exception as e:
        logging.error(f"Error finding latest URL: {e}")
        raise


import time

def scrape_page(page_url):
    """爬取单一页面并返回精确的日期数据."""
    max_retries = 1
    retry_delay = 30  # seconds

    for attempt in range(max_retries):
        try:
            response = requests.get(page_url, cookies={'over18': '1'})  # PTT requires an over-18 cookie for some boards
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            post_links = ['https://www.ptt.cc' + a['href'] for a in soup.select('.r-ent .title a')]

            if not post_links:
                return None

            def get_full_date(url):
                resp = requests.get(url, cookies={'over18': '1'})
                resp.raise_for_status()
                page_soup = BeautifulSoup(resp.text, 'html.parser')
                date_line = page_soup.find('span', string='時間').find_next('span', class_='article-meta-value').text
                # Parsing date in the format 'Wed Apr 24 13:36:53 2024'
                
                return datetime.strptime(date_line, '%a %b %d %H:%M:%S %Y')

            start_date = get_full_date(post_links[0])
            end_date = get_full_date(post_links[-1])

            return {'URL': page_url, 'Start_Date': start_date, 'End_Date': end_date}
        except Exception as e:
            logging.error(f"Error scraping page {page_url} on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:  # prevent sleeping after the last attempt
                time.sleep(retry_delay)
            else:
                return None

def parallel_scrape(**kwargs):
    """平行抓取并写入 Json，同一页信息要写在同一个 Json Block中."""
    latest_url = kwargs['ti'].xcom_pull(task_ids='get_latest_url')
    last_page_number = int(latest_url.split('index')[1].split('.html')[0])
    page_urls = [f'https://www.ptt.cc/bbs/MacShop/index{i}.html' for i in range(1, last_page_number + 1)]

    with ThreadPoolExecutor(max_workers=40) as executor:
        results = list(executor.map(scrape_page, page_urls))

    # Filter out None results and ensure results are written to JSON in a newline-delimited format
    valid_results = [result for result in results if result]

    with open('/tmp/macshop_index_and_dates_mapping.json', 'w', encoding='utf-8') as f:
        for result in valid_results:
            json_record = json.dumps(result, default=str)  # Serialize each result as a JSON string
            f.write(json_record + '\n')  # Write each JSON record on a new line

get_latest_url = PythonOperator(
    task_id='get_latest_url',
    python_callable=find_latest_url,
    dag=dag
)

scrape_and_save = PythonOperator(
    task_id='parallel_scrape_and_save',
    python_callable=parallel_scrape,
    provide_context=True,
    dag=dag
)

bucket_name = 'macshop_data'
upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/macshop_index_and_dates_mapping.json',
    dst='macshop_index_and_dates_mapping.json',
    bucket=bucket_name,
    gcp_conn_id="Connect_Goolge",
    dag=dag
)

schema_fields = [
    {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Start_Date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'End_Date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
]

load_json_to_bigquery = GCSToBigQueryOperator(
    task_id='load_json_to_bigquery',
    bucket=bucket_name,
    source_objects=['macshop_index_and_dates_mapping.json'],
    destination_project_dataset_table='encoded-axis-415404.Macshop_Data.Info_Macshop_Index_and_Dates_Mapping',
    write_disposition='WRITE_TRUNCATE',
    schema_fields=schema_fields,
    source_format='NEWLINE_DELIMITED_JSON',
    gcp_conn_id="Connect_Goolge",
    time_partitioning={'type': 'DAY', 'field': 'Start_Date'},  # Partition by date_info
    cluster_fields=['Start_Date'],
    dag=dag,
)

get_latest_url >> scrape_and_save >> upload_to_gcs >> load_json_to_bigquery