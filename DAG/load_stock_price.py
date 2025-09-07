import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from google.cloud import storage, bigquery
import pandas as pd
import io

# Derive yesterday's date
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default args
default_args = {
    'start_date': yesterday,
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

# Load stock price data into BigQuery
def load_stock_price_to_bigquery(bucket_name="ds-4004-data-bucket", blob_name="stock_price"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))

    # --- Normalize & rename columns ---
    df.columns = df.columns.str.strip()  # remove any accidental spaces

    rename_map = {
        "symbol": "company",
        "from": "from_date",
        "to": "to_date",
        "c": "current_price",
        "d": "price_diff",
        "dp": "diff_percentage",
        "h": "high",
        "l": "low",
        "o": "open",
        "pc": "close"
    }
    df.rename(columns=rename_map, inplace=True)

    # --- Ensure all required columns exist ---
    expected_columns = [
        "company", "from_date", "to_date", "current_price",
        "price_diff", "diff_percentage", "high", "low", "open", "close"
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None  # fill missing with NULLs

    # --- Fix date formats ---
    if "from_date" in df.columns:
        df["from_date"] = pd.to_datetime(df["from_date"], errors="coerce").dt.date
    if "to_date" in df.columns:
        df["to_date"] = pd.to_datetime(df["to_date"], errors="coerce").dt.date

    # --- BigQuery load ---
    client_bq = bigquery.Client()
    project_id = "ds-4004-group-project"
    dataset_id = "ds_4004_stock_data"
    table_id = "ds_4004_stock_price"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("company", "STRING"),
            bigquery.SchemaField("from_date", "DATE"),
            bigquery.SchemaField("to_date", "DATE"),
            bigquery.SchemaField("current_price", "FLOAT"),
            bigquery.SchemaField("price_diff", "FLOAT"),
            bigquery.SchemaField("diff_percentage", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
        ]
    )

    job = client_bq.load_table_from_dataframe(df[expected_columns], full_table_id, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}")

# --- DAG Definition ---
with DAG(
    "Stock_Price_Pipeline",
    default_args=default_args,
    schedule_interval="35 0 * * *",  # runs 5 min after news pipeline
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    store_stock_price_to_bigquery = PythonOperator(
        task_id="store_stock_price_to_bigquery",
        python_callable=load_stock_price_to_bigquery
    )

    end = DummyOperator(task_id='end')

    start >> store_stock_price_to_bigquery >> end
