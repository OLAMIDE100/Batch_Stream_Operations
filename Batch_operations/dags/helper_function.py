import json as js

import boto3

with open("/usr/local/airflow/dags/key.json") as f:
    key = js.load(f)


def download_csv():

    bucket = "bigspark.challenge.data"

    sales_file = "tpcds_data_5g_batch/batch_2000-01-01/store_sales/part-00000-f23e8699-a89d-4721-96e4-a740242ee3f3-c000.csv"
    store_file = "tpcds_data_5g_batch/batch_2000-01-01/store/part-00000-57bc87c3-bc5e-439c-9438-94bb05da75db-c000.csv"
    item_file = "tpcds_data_5g_batch_updated/batch_2000-01-01/item/part-00000-e8aa9f6d-2cc4-4485-bf76-d98d8dd38f42-c000.csv"
    date_file = "tpcds_data_5g_batch/batch_2000-01-01/date_dim/part-00000-8dc7cb65-ebef-4188-9277-45218366a270-c000.csv"

    sales_path = "/usr/local/spark/resources/data/input/sales.csv"
    store_path = "/usr/local/spark/resources/data/input/store.csv"
    item_path = "/usr/local/spark/resources/data/input/item.csv"
    date_path = "/usr/local/spark/resources/data/input/date.csv"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=key["a_key"],
        aws_secret_access_key=key["s_key"],
    )

    print("######################################")
    print("Downloading CSV FILE")
    print("######################################")

    s3.download_file(bucket, sales_file, sales_path)
    s3.download_file(bucket, store_file, store_path)
    s3.download_file(bucket, item_file, item_path)
    s3.download_file(bucket, date_file, date_path)

    print("######################################")
    print("Extraction  COMPLETED")
    print("######################################")
