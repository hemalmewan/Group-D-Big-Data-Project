import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from google.cloud import storage, language_v1, bigquery
import pandas as pd
import io
import time

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

# Load the stock news from the GCS bucket
def load_stock_news_from_gcs(bucket_name="ds-4004-data-bucket", blob_name="news_headline"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))

    return df.to_dict("records")

# Perform sentiment analysis using Vertex AI
def perform_sentiment_analysis(**context):
    news_data = context['ti'].xcom_pull(task_ids='load_stock_news_from_gcs')

    client = language_v1.LanguageServiceClient()
    sentiment_result = []

    for row in news_data:
        headline = row['headline']
        symbol = row['related']

        if not isinstance(headline, str) or not headline.strip():
            continue

        document = language_v1.Document(
            content=headline,
            type_=language_v1.Document.Type.PLAIN_TEXT,
        )

        response = client.analyze_sentiment(request={"document": document})
        sentiment_score = response.document_sentiment.score

        sentiment_result.append({
            "from": row['from'],
            "to": row["to"],
            "symbol": symbol,
            "headline": headline,
            "sentiment_score": sentiment_score
        })

        time.sleep(0.1)  # 100ms delay between API calls

    context['ti'].xcom_push(key="sentiment_data", value=sentiment_result)

# Categorize sentiment based on score
def categorize_sentiment(**context):
    sentiment_data = context['ti'].xcom_pull(task_ids='vertex_ai_sentiment', key='sentiment_data')

    if not sentiment_data:
        raise ValueError("No sentiment data to categorize")

    df = pd.DataFrame(sentiment_data)

    def get_category(score):
        if score > 0.1:
            return "Positive"
        elif score < -0.1:
            return "Negative"
        else:
            return "Neutral"

    df['sentiment_category'] = df['sentiment_score'].apply(get_category)

    context['ti'].xcom_push(key='sentiment_data_categorized', value=df.to_dict('records'))

# Load data into BigQuery
def load_to_bigquery(**context):
    sentiment_data = context['ti'].xcom_pull(task_ids="categorize_sentiment", key="sentiment_data_categorized")

    if not sentiment_data:
        raise ValueError("No sentiment data available to load to BigQuery")

    df = pd.DataFrame(sentiment_data)

    # Rename 'from' to avoid SQL keyword issues
    if "from" in df.columns:
        df.rename(columns={"from": "from_date"}, inplace=True)
    
    if "to" in df.columns:
        df.rename(columns={"to": "to_date"}, inplace=True)
    
    if "symbol" in df.columns:
        df.rename(columns={"symbol": "company"}, inplace=True)

    # Ensure proper date formats
    if "from_date" in df.columns:
        df["from_date"] = pd.to_datetime(df["from_date"], errors="coerce").dt.date
    if "to_date" in df.columns:
        df["to_date"] = pd.to_datetime(df["to_date"], errors="coerce").dt.date

    client = bigquery.Client()
    project_id = "ds-4004-group-project"
    dataset_id = "ds_4004_stock_data"
    table_id = "ds_4004_news_headline"
    task_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("company", "STRING"),
            bigquery.SchemaField("from_date", "DATE"),
            bigquery.SchemaField("to_date", "DATE"),
            bigquery.SchemaField("headline", "STRING"),
            bigquery.SchemaField("sentiment_score", "FLOAT"),
            bigquery.SchemaField("sentiment_category", "STRING"),
        ]
    )

    job = client.load_table_from_dataframe(df, task_id, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}")

# DAG definition
with DAG(
    "Data_Orchestration_Pipeline",
    default_args=default_args,
    schedule_interval="37 0 * * *",
    catchup=False
) as dag:

    start = DummyOperator(task_id='start', dag=dag)

    load_stock_news_task = PythonOperator(
        task_id="load_stock_news_from_gcs",
        python_callable=load_stock_news_from_gcs,
        dag=dag
    )

    vertex_ai_sentiment = PythonOperator(
        task_id="vertex_ai_sentiment",
        python_callable=perform_sentiment_analysis,
        dag=dag
    )

    categorize_sentiment_task = PythonOperator(
        task_id="categorize_sentiment",
        python_callable=categorize_sentiment,
        dag=dag
    )

    store_to_bigquery = PythonOperator(
        task_id="store_to_bigquery",
        python_callable=load_to_bigquery,
        dag=dag
    )

    end = DummyOperator(task_id='end', dag=dag)

    # DAG workflow
    start >> load_stock_news_task >> vertex_ai_sentiment >> categorize_sentiment_task >> store_to_bigquery >> end



