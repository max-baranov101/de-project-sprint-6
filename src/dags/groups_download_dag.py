from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

def download_s3_file():
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key='users.csv',
        Filename='/data/users.csv'
    )

default_args = {
    'owner': 'meow',
    'start_date': datetime(2024, 4, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'groups_download',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:

    download_task = PythonOperator(
        task_id='download_s3_file',
        python_callable=download_s3_file,
    )

download_task


# -----------------------------------------------------------------------------------------------
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.decorators import dag

# import boto3
# import pendulum


# AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
# AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

# def fetch_s3_file(bucket: str, key: str) -> str:
#     session = boto3.session.Session()
#     s3_client = session.client(
#         service_name='s3',
#         endpoint_url='https://storage.yandexcloud.net',
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#     )
#     s3_client.download_file(
# 				Bucket=bucket,
# 				Key=key,
# 				Filename=f'/data/{key}'
# )


# bash_command_tmpl = """
# head {{ files }}
# """

# @dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
# def sprint6_project_dag_get_data():
#     bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv' )
#     fetch_tasks = [
#         PythonOperator(
#             task_id=f'fetch_{key}',
#             python_callable=fetch_s3_file,
#             op_kwargs={'bucket': 'sprint6', 'key': key},
#         ) for key in bucket_files
#     ]

#     print_10_lines_of_each = BashOperator(
#         task_id='print_10_lines_of_each',
#         bash_command=bash_command_tmpl,
#         params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
#     )

#     fetch_tasks >> print_10_lines_of_each

# _ = sprint6_project_dag_get_data()