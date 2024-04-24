from datetime import datetime

from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from helpers import s3_download as s3
from helpers import csv_test as csv
from helpers import vertica_copy as vertica

vertica_conn = BaseHook.get_connection(conn_id="de_vertica")
s3_hook = S3Hook("de_ycloud_s3")
staging_schema = "stv202404101__STAGING"


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 17),
    catchup=False,
)
def sprint6():
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="staging") as staging:

        @task
        def get_users():
            s3.download_file(key="users.csv", hook=s3_hook)

        @task
        def get_groups():
            s3.download_file(key="groups.csv", hook=s3_hook)

        @task
        def get_dialogs():
            s3.download_file(key="dialogs.csv", hook=s3_hook)

        @task
        def test_users_csv():
            csv.test_csv(file="/data/users.csv")

        @task
        def test_groups_csv():
            csv.test_csv(file="/data/groups.csv")

        @task
        def test_dialogs_csv():
            csv.test_csv(file="/data/dialogs.csv")

        @task
        def load_users():
            vertica.copy_from_local(
                from_csv_path="/data/users.csv",
                to_db=f"{staging_schema}.users",
                columns=[
                    "id",
                    "chat_name ENFORCELENGTH",
                    "registration_dt",
                    "country ENFORCELENGTH",
                    "age",
                ],
                conn=vertica_conn,
            )

        @task
        def load_groups():
            vertica.copy_from_local(
                from_csv_path="/data/groups.csv",
                to_db=f"{staging_schema}.groups",
                columns=[
                    "id",
                    "admin_id",
                    "group_name ENFORCELENGTH",
                    "registration_dt",
                    "is_private",
                ],
                conn=vertica_conn,
            )

        @task
        def load_dialogs():
            vertica.copy_from_local(
                from_csv_path="/data/dialogs.csv",
                to_db=f"{staging_schema}.dialogs",
                columns=[
                    "message_id",
                    "message_ts",
                    "message_from",
                    "message_to",
                    "message ENFORCELENGTH",
                    "group_filler FILLER NUMERIC",
                    "message_group AS group_filler::INT",
                ],
                conn=vertica_conn,
            )
      
        test_users_csv = test_users_csv()
        test_groups_csv = test_groups_csv()
        test_dialogs_csv = test_dialogs_csv()

        load_users = load_users()
        load_groups = load_groups()
        load_dialogs = load_dialogs()
        
        """
        get_users = get_users()
        get_groups = get_groups()
        get_dialogs = get_dialogs()
        
        get_users >> test_users_csv >> load_users
        get_groups >> test_groups_csv >> load_groups
        get_dialogs >> test_dialogs_csv >> load_dialogs
        """
        
        test_users_csv >> load_users
        test_groups_csv >> load_groups
        test_dialogs_csv >> load_dialogs

    end = EmptyOperator(task_id="end")

    start >> staging >> end


dag = sprint6()


# 3.2.1 Двигайтесь дальше! Ваш код: NSVMjNvvxQ
# 3.2.2 Двигайтесь дальше! Ваш код: gPXvR0F9IW
# 3.3.2 Двигайтесь дальше! Ваш код: psfIG0C6s7
