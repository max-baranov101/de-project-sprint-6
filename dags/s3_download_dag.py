from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

# Инициализация сессии boto3 один раз
session = boto3.session.Session()

# Значения AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY перенесены в Variables в Airflow
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def download_s3_file(file_name):
    s3_client.download_file(
        Bucket='sprint6',
        Key=file_name,
        Filename=f'/data/{file_name}'
    )

default_args = {
    'owner': 'Maxim Baranov',
    'start_date': datetime(2024, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    's3_download',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    files = ['dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv']
    for file_name in files:
        PythonOperator(
            task_id=f'download_{file_name.replace(".", "_")}',
            python_callable=download_s3_file,
            op_kwargs={'file_name': file_name}
        )


"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

# Значения AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY перенесены в Variables в Airflow
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

def download_s3_file(file_name):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=file_name,
        Filename=f'/data/{file_name}'
    )

default_args = {
    'owner': 'Maxim Baranov',
    'start_date': datetime(2024, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    's3_download',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    files = ['dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv']
    tasks = {}
    for file_name in files:
        task = PythonOperator(
            task_id=f'download_{file_name.replace(".", "_")}',
            python_callable=download_s3_file,
            op_kwargs={'file_name': file_name},
        )
        tasks[file_name] = task

for task in tasks.values():
    task
"""