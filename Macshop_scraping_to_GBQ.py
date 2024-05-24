from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Default parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'Macshop_scraping_to_GBQ',
    default_args=default_args,
    description='從Google BigQuery表格提取PTT頁面URL並抓取文章數據，再存儲到Google Cloud Storage',
    schedule_interval=None,
)

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(30),
    retry=retry_if_exception_type(Exception)
)
def get_soup(url, **kwargs):
    try:
        response = requests.get(url, cookies={'over18': '1'}, timeout=10)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        else:
            print(f"Error fetching page: Status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_article_content(article_url, **kwargs):
    try:
        soup = get_soup(article_url)
        if soup is None:
            return article_url, "無法檢索文章內容", "日期資訊未找到", ""

        date_info = soup.find_all('div', class_='article-metaline')[-1].find('span', class_='article-meta-value').text if soup.find_all('div', 'article-metaline') else "日期資訊未找到"

        try:
            post_date = datetime.strptime(date_info, '%a %b %d %H:%M:%S %Y')
            formatted_date = post_date.strftime('%Y-%m-%d')
        except ValueError:
            formatted_date = "日期資訊未找到"

        content = soup.find(id='main-content')
        for meta in content.select('div.article-metaline, div.article-metaline-right, div.push'):
            meta.decompose()

        content_text = content.text.strip()

        # 提取完整文章內容
        full_content = content_text

        return article_url, "完整文章內容", formatted_date, full_content
    except Exception as e:
        print(f"Error processing article {article_url}: {e}")
        return article_url, "無法檢索文章內容", "日期資訊未找到", ""

def extract_urls_from_bigquery(**kwargs):
    hook = BigQueryHook(gcp_conn_id="Connect_Goolge", use_legacy_sql=False)
    client = hook.get_client()

    query = """
        SELECT URL
        FROM `encoded-axis-415404.Macshop_Data.Info_Macshop_Index_and_Dates`
        WHERE Start_Date >= '2000-01-01'
    """
    query_job = client.query(query)
    results = query_job.result()
    urls = [row.URL for row in results]

    print(f"Extracted URLs: {urls}")
    kwargs['ti'].xcom_push(key='urls', value=urls)

extract_urls_task = PythonOperator(
    task_id='extract_urls_from_bigquery',
    python_callable=extract_urls_from_bigquery,
    dag=dag,
)

def scrape_articles_and_save_to_csv(**kwargs):
    urls = kwargs['ti'].xcom_pull(task_ids='extract_urls_from_bigquery', key='urls')
    if not urls:
        print("No URLs extracted from BigQuery.")
        kwargs['ti'].xcom_push(key='csv_file', value='')
        return

    results = []

    def process_url(current_url):
        soup = get_soup(current_url)
        if soup is None:
            print(f"Skipping URL {current_url} due to fetch error")
            return []

        posts = soup.find_all('div', class_='r-ent')
        local_results = []
        for post in posts:
            title_link = post.find('div', class_='title').a
            if title_link is None:
                continue
            title = title_link.text.strip()
            article_url = 'https://www.ptt.cc' + title_link['href']
            article_url, _, creation_date, full_content = get_article_content(article_url)
            # 替换换行符
            full_content = full_content.replace('\n', ' ').replace('\r', ' ')
            local_results.append([article_url, title, creation_date, full_content])

            print(f"URL: {article_url}")
            print(f"標題: {title}")
            print(f"創建日期: {creation_date}")
            print(f"完整文章內容: {full_content}")
            print("----------------")
        return local_results

    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(process_url, url) for url in urls]
        for future in as_completed(futures):
            try:
                local_results = future.result()
                if local_results:
                    results.extend(local_results)
            except Exception as e:
                print(f"Error processing URL: {e}")

    print(f"Total URLs processed: {len(urls)}")
    print(f"Total articles processed: {len(results)}")
    csv_file = '/tmp/ptt_scraped_url_and_content.csv'
    if results:
        with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_ALL)  # 确保所有字段都被引号包围
            writer.writerow(['URL', 'Title', 'Creation_Date', 'Full_Content'])
            writer.writerows(results)
        print(f"Data successfully written to {csv_file}")
        kwargs['ti'].xcom_push(key='csv_file', value=csv_file)
    else:
        print("No articles found.")
        kwargs['ti'].xcom_push(key='csv_file', value='')

scrape_articles_task = PythonOperator(
    task_id='scrape_articles_and_save_to_csv',
    python_callable=scrape_articles_and_save_to_csv,
    dag=dag,
)

upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/ptt_scraped_url_and_content.csv',
    dst='ptt_scraped_url_and_content.csv',
    bucket='macshop_data',
    gcp_conn_id="Connect_Goolge",
    dag=dag
)

load_csv_to_bigquery_task = GCSToBigQueryOperator(
    task_id='load_csv_to_bigquery',
    bucket='macshop_data',
    source_objects=['ptt_scraped_url_and_content.csv'],
    destination_project_dataset_table='encoded-axis-415404.Macshop_Data.Log_Macshop_url',
    schema_fields=[
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Creation_Date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'Full_Content', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,  # 跳过标题行
    time_partitioning={'type': 'DAY', 'field': 'Creation_Date'},
    cluster_fields=['Creation_Date'],
    field_delimiter=',',  # 确保分隔符与 CSV 文件一致
    max_bad_records=100,
    gcp_conn_id="Connect_Goolge",
    dag=dag
)

extract_urls_task >> scrape_articles_task >> upload_to_gcs_task >> load_csv_to_bigquery_task
