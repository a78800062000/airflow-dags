import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
from tempfile import NamedTemporaryFile
import os
from google.cloud import storage  # 導入Google Cloud Storage客戶端庫

def fetch_and_save_data(start_date, end_date, bucket_name, destination_blob_name):
    # 建立Google Cloud Storage客戶端實例
    storage_client = storage.Client()
    # 獲取指定的bucket
    bucket = storage_client.bucket(bucket_name)
    # 指定要寫入的blob名稱
    blob = bucket.blob(destination_blob_name)

    def get_soup(url):
        try:
            response = requests.get(url, cookies={'over18': '1'}, timeout=10)
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            else:
                print(f"錯誤: 狀態碼 {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"請求失敗: {e}")
            return None

    def get_previous_page_url(soup):
        controls = soup.find('div', class_='action-bar').find('div', class_='btn-group btn-group-paging')
        prev_link = controls.find_all('a')[1]['href'] if controls else None
        if prev_link:
            return 'https://www.ptt.cc' + prev_link
        return None

    def get_article_content(article_url):
        soup = get_soup(article_url)
        if soup is None:
            return "無法檢索文章內容", "日期資訊未找到"

        date_info = soup.find_all('div', class_='article-metaline')[-1].find('span', class_='article-meta-value').text if soup.find_all('div', class_='article-metaline') else "日期資訊未找到"

        content = soup.find(id='main-content')
        for meta in content.select('div.article-metaline, div.article-metaline-right, div.push'):
            meta.decompose()
        content_text = content.text.strip()

        try:
            start_index = content_text.index('[售價]')
            end_index = content_text.index('[交易方式/地點]', start_index)
            price_info = content_text[start_index:end_index].strip()
            return price_info, date_info
        except ValueError:
            return "文章中未找到價格資訊或交易方式資訊。", date_info

    # 初始化為 MacShop 版最新頁面的 URL
    current_url = 'https://www.ptt.cc/bbs/MacShop/index.html'

    data = []

    while current_url:
        soup = get_soup(current_url)
        if soup is None:
            print("無法獲取網頁內容，跳過此次迭代")
            break

        should_stop = False
        posts = soup.find_all('div', class_='r-ent')
        for post in posts:
            try:
                title_link = post.find('div', class_='title').a
                if title_link is None:
                    continue
                title = title_link.text.strip()
                date_str = post.find('div', class_='date').text.strip()
                post_date = datetime.strptime(date_str + ' 2024', '%m/%d %Y')

                if post_date < start_date:
                    should_stop = True
                    break

                if '[販售]' in title and '128' in title and 'pro' not in title.lower() and 'mini' not in title.lower() and 'iPhone 13' in title:
                    article_url = 'https://www.ptt.cc' + title_link['href']
                    price_info, creation_date = get_article_content(article_url)
                    data.append({
                        "URL": article_url,
                        "標題": title,
                        "價格": price_info,
                        "創建日期": creation_date
                    })
            except Exception as e:
                print(f"解析帖子時發生錯誤: {e}")
                continue

        if should_stop:
            break

        current_url = get_previous_page_url(soup)

    # 將數據轉換為CSV格式的字符串
    data_string = "URL,標題,價格,創建日期\n"
    for item in data:
        data_string += f'{item["URL"]},{item["標題"]},{item["價格"]},{item["創建日期"]}\n'

    # 將數據字符串上傳到GCS
    blob.upload_from_string(data_string, content_type='text/csv')

# 更新執行任務的函數，以使用新的引數
def task_fetch_and_save_data():
    start_date = datetime.strptime('2024-03-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-03-23', '%Y-%m-%d')
    
    # 設定GCS的桶名和blob名稱
    bucket_name = 'macshop_data'
    destination_blob_name = f'{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}.csv'
    
    # 呼叫改寫後的fetch_and_save_data函數，直接將數據上傳至GCS
    fetch_and_save_data(start_date, end_date, bucket_name, destination_blob_name)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    'fetch_data_and_upload_gcs',  # DAG的唯一ID
    default_args=default_args,    # 应用到所有任务的默认参数
    description='Fetch data from PTT and upload directly to GCS',  # DAG描述
    schedule_interval=timedelta(days=1),  # 调度间隔
)

# 定义用于执行 fetch_and_save_data 的任务
fetch_save_data_task = PythonOperator(
    task_id='fetch_and_save_data',  # 任务的唯一ID
    python_callable=task_fetch_and_save_data,  # 指向你的函数
    dag=dag,  # 指定这个任务属于哪个DAG
)
