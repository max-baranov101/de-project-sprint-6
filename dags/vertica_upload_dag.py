from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import vertica_python
from typing import Dict  # Corrected import here

def sql_execute(*, sql: str, conn_info: Dict[str, str]):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

def load_to_vertica(file_name, table_name, columns, vertica_conn):
    # Construct SQL for loading data to Vertica
    columns_list = ",".join(columns)
    sql = f"""
        COPY {staging_schema}.{table_name} ( {columns_list} )
        FROM LOCAL '/data/{file_name}.csv'
        DELIMITER ','
        ENCLOSED BY '"'
        NO ESCAPE
        REJECTED DATA AS TABLE {staging_schema}.{table_name}_rej
        ;
    """
    conn_info = {
        "host": vertica_conn.host,
        "port": vertica_conn.port,
        "user": vertica_conn.login,
        "password": vertica_conn.password,
        "database": vertica_conn.schema,
    }
    sql_execute(sql=sql, conn_info=conn_info)

default_args = {
    'start_date': datetime(2024, 4, 17),
    'catchup': False,
}

with DAG('vertica_upload', schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    vertica_conn = BaseHook.get_connection(conn_id="de_vertica")
    staging_schema = "STV202404101__STAGING"

    with TaskGroup("load_files") as load_files:
        @task
        def load_users():
            load_to_vertica("users", "users", [
                "id", "chat_name ENFORCELENGTH", "registration_dt", "country ENFORCELENGTH", "age"
            ], vertica_conn)

        @task
        def load_groups():
            load_to_vertica("groups", "groups", [
                "id", "admin_id", "group_name ENFORCELENGTH", "registration_dt", "is_private"
            ], vertica_conn)

        @task
        def load_dialogs():
            load_to_vertica("dialogs", "dialogs", [
                "message_id", "message_ts", "message_from", "message_to", "message ENFORCELENGTH", "group_filler FILLER NUMERIC", "message_group AS group_filler::INT"
            ], vertica_conn)

        @task
        def load_group_log():
            load_to_vertica("group_log", "group_log", [
                "group_id", "user_id", "user_id_from", "event", "datetime"
            ], vertica_conn)

        users = load_users()
        groups = load_groups()
        dialogs = load_dialogs()
        group_log = load_group_log()

        users >> groups >> dialogs >> group_log

    start >> load_files >> end