import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def download_file(
    *,
    key: str,
    hook: S3Hook,
    bucket_name: str = "sprint6",
    local_path: str = "/data/"
):
    file = hook.download_file(
        key=key, bucket_name=bucket_name, local_path=local_path
    )

    os.rename(src=file, dst=local_path + key)


if __name__ == "__main__":
    hook = S3Hook("de_ycloud_s3")
    download_file(key="groups.csv", hook=hook)
