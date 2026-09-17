import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
 
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator

# if using a key-pair auth instead of password
from cryptography.hazmat.primitives import serialization
 
RUSTFS_ENDPOINT = "http://rustfs:9000"
RUSTFS_ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
RUSTFS_SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]
BUCKET = "bronze-transactions"
LOCAL_DIR = Path("/tmp/stock_pipeline")
 
## if using a key-pair auth instead of password
with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as key_file:
    p_key = serialization.load_pem_private_key(key_file.read(), password=None)
 
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)
##


SNOWFLAKE_CONFIG = {
    "user": os.environ["SNOWFLAKE_USER"],

    #"password": os.environ["SNOWFLAKE_PASSWORD"],
    "private_key": pkb,  # instead of "password": os.environ["SNOWFLAKE_PASSWORD"]
    
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
}
 
def download_from_rustfs(**context):
    run_local_dir = LOCAL_DIR / context["run_id"].replace(":", "_").replace("+", "_")
    if run_local_dir.exists():
        shutil.rmtree(run_local_dir)
    run_local_dir.mkdir(parents=True, exist_ok=True)
 
    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
        region_name="us-east-1",
    )
 
    # list_objects_v2 caps a single call at 1000 keys, returned in
    # lexicographic order. An unpaginated call would only ever see the
    # first (alphabetically-earliest) symbol once its object count passed
    # 1000, never reaching the rest. The paginator walks every page.
    paginator = s3.get_paginator("list_objects_v2")
 
    files = []
    keys = []
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Flatten "SYMBOL/167.json" into "SYMBOL_167.json" rather than
            # nested folders. Snowflake's PUT stages files by basename
            # alone, dropping local folder structure -- so nested
            # per-symbol folders sharing a bare numeric filename (easily
            # produced when multiple symbols are fetched within the same
            # wall-clock second, which happens on every producer cycle)
            # would otherwise collide in the internal stage. PUT silently
            # skips re-staging a name that already exists rather than
            # erroring, so every symbol but whichever staged first for
            # that timestamp would silently never make it into Snowflake.
            destination = run_local_dir / key.replace("/", "_")
            s3.download_file(BUCKET, key, str(destination))
            files.append(str(destination))
            keys.append(key)
            print(f"Downloaded {key} -> {destination}")
 
    context["ti"].xcom_push(key="downloaded_files", value=files)
    context["ti"].xcom_push(key="downloaded_keys", value=keys)
    context["ti"].xcom_push(key="local_dir", value=str(run_local_dir))
 
 
def load_into_snowflake(**context):
    files = context["ti"].xcom_pull(
        task_ids="download_rustfs", key="downloaded_files"
    )
    keys = context["ti"].xcom_pull(task_ids="download_rustfs", key="downloaded_keys")
    local_dir = context["ti"].xcom_pull(task_ids="download_rustfs", key="local_dir")
    if not files:
        print("No RustFS files found.")
        return
 
    connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = connection.cursor()
    try:
        # Explicitly set session context rather than trusting connect()'s
        # database/schema parameters to have taken effect silently. Some
        # connector versions don't validate them at connect time, and skip
        # setting the context without erroring if they're wrong, which
        # surfaces later as "this session does not have a current schema".
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS BRONZE_STOCK_QUOTES_RAW (
                V VARIANT
            )
            """
        )
        for file_path in files:
            normalized_path = Path(file_path).as_posix()
            print(f"Uploading {normalized_path}")
            cursor.execute(
                f"PUT file://{normalized_path} "
                "@%BRONZE_STOCK_QUOTES_RAW "
                "AUTO_COMPRESS=TRUE"
            )
 
        cursor.execute(
            """
            COPY INTO BRONZE_STOCK_QUOTES_RAW
            FROM @%BRONZE_STOCK_QUOTES_RAW
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = 'CONTINUE'
            """
        )
        for result in cursor.fetchall():
            print(result)
 
        # Everything above succeeded without raising, so every file
        # downloaded this during this run was both PUT and included in the COPY INTO
        # batch. Delete them from RustFS now so the bucket doesn't grow
        # forever and future runs don't keep re-listing/re-downloading/
        # re-uploading an ever-growing backlog, which blocks out
        # symbols that ahow later according to sort order (e.g. AAPL's backlog blocking
        # AMZN/GOOGL/MSFT/TSLA from ever being reached in time before deletion).
        s3_delete = boto3.client(
            "s3",
            endpoint_url=RUSTFS_ENDPOINT,
            aws_access_key_id=RUSTFS_ACCESS_KEY,
            aws_secret_access_key=RUSTFS_SECRET_KEY,
            region_name="us-east-1",
        )
        for key in keys:
            s3_delete.delete_object(Bucket=BUCKET, Key=key)
        print(f"Deleted {len(keys)} processed object(s) from RustFS.")
    finally:
        cursor.close()
        connection.close()
        if local_dir and Path(local_dir).exists():
            shutil.rmtree(local_dir)
            print(f"Cleaned up {local_dir}")
 
 
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
 
with DAG(
    dag_id="stocks_storage_to_snowflake",
    default_args=default_args,
    description="Load RustFS stock JSON files into Snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "snowflake", "rustfs"],
) as dag:
 
    download_task = PythonOperator(
        task_id="download_rustfs",
        python_callable=download_from_rustfs,
    )
 
    snowflake_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_into_snowflake,
    )
 
    download_task >> snowflake_task