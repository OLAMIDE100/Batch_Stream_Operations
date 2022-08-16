import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from helper_function import download_csv

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "transformation"
input_1 = "/usr/local/spark/resources/data/input/sales.csv"
input_2 = "/usr/local/spark/resources/data/input/store.csv"
input_3 = "/usr/local/spark/resources/data/input/date.csv"
input_4 = "/usr/local/spark/resources/data/input/item.csv"
output = "/usr/local/spark/resources/data/output"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "bigspark_endless-context-338620")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    print("######################################")
    print("Successfully Transfered to Google Cloud Bucket")
    print("######################################")


###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="spark-test",
    description="This DAG runs a simple Pyspark app.",
    default_args=default_args,
    schedule_interval=timedelta(1),
)


extract_job = PythonOperator(task_id="extract", python_callable=download_csv, dag=dag)


transform_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/transform.py",  # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[input_1, input_2, input_3, input_4, output],
    dag=dag,
)


prepare_path = BashOperator(
    task_id="prepare_path",
    bash_command=f"mv /usr/local/spark/resources/data/output/*.csv /usr/local/spark/resources/data/output/result.csv",
    dag=dag,
)


local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": f"result_2020_01_01.csv",
        "local_file": "/usr/local/spark/resources/data/output/result.csv",
    },
    dag=dag,
)

remove_files = BashOperator(
    task_id="remove_files",
    bash_command="rm  /usr/local/spark/resources/data/input/*.csv",
    dag=dag,
)

extract_job >> transform_job >> prepare_path >> local_to_gcs_task >> remove_files
