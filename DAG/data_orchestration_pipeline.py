import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
from airflow.operators.dummy import DummyOperator
from google.cloud import storage
import pandas as pd
import io
from google.cloud import language_v1
from airflow.models import Variable
from google.cloud import bigquery
import time


##derive the yeaterday date
yesterday=datetime.combine(datetime.today()-timedelta(1),datetime.min.time())

##default args
default_args={
    'start_date':yesterday,
    'retries':1, ##number iteration should task run when it is fail
    'email_on_failure':False,##send the email whatever task is fail
    'email_on_retry':False,##again send the email when the task is again run and again fail
    'retry_delay':timedelta(minutes=5)##task execution time when the particular task is failure
}

##define the custom python callable functions 

##load the stock news from the gcs bucket
def load_stock_news_from_gcs(bucket_name="ds_4004_d_bucket",blob_name="stock_news"):
    client=storage.Client()
    bucket=client.bucket(bucket_name) ##store the bucket
    blob=bucket.blob(blob_name) ##store the csv file

    data=blob.download_as_bytes()
    df=pd.read_csv(io.BytesIO(data))

    ##Return as dictionary or save to XCom
    return df.to_dict("records")


##performe the sentiment analysis part using vertext AI
def perform_sentiment_analysis(**context):
    ##Get the news data(passed via Xcom or global read if saved somewhere)
    news_data=context['ti'].xcom_pull(task_ids='load_stock_news_from_gcs')

    client=language_v1.LanguageServiceClient()

    sentiment_result=[]
    for row in news_data:
        headline=row['headline']
        symbol=row['related']

        if not headline:
            continue

        document=language_v1.Document(
            content=headline,
            type_=language_v1.Document.Type.PLAIN_TEXT,
            language="en"
        
        )

        response=client.analyze_sentiment(request={"document":document})
        sentiment_score=response.document_sentiment.score ##Range -1 to 1


        sentiment_result.append({
            "from":row['from'],
            "to":row["to"],
            "symbol":symbol,
            "headline":headline,
            "sentiment_score":sentiment_score
        })

        time.sleep(0.1) ##100ms between calls

    ##Store the result in BigQuery Table
    context['ti'].xcom_push(key="sentiment_data",value=sentiment_result)


##data laod to the bigquery table
def load_to_bigquery(**context):
    ##pull enrich data from XCom
    sentiment_data=context['ti'].xcom_pull(task_ids="vertex_ai_sentiment",key="sentiment_data")

    if not sentiment_data:
        raise ValueError("No sentiment data available to load to BigQuery")


    df=pd.DataFrame(sentiment_data) ##convert the dataframe

    #Rename 'from' to avoid SQL keyword issues
    if "from" in df.columns:
        df.rename(columns={"from": "from_date"}, inplace=True)

    #Ensure proper date formats
    if "from_date" in df.columns:
        df["from_date"] = pd.to_datetime(df["from_date"], errors="coerce").dt.date
    if "to" in df.columns:
        df["to"] = pd.to_datetime(df["to"], errors="coerce").dt.date

    ##BigQuery setup
    client=bigquery.Client()
    
    ##configure the bigquery credintials
    project_id="ds-4004-d-big-data-project"
    dataset_id="ds_4004_d_stock_data"
    table_id="ds_4004_d_stock_table"

    task_id=f"{project_id}.{dataset_id}.{table_id}"

    ##define the schema of the bigquery table
    job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("from_date","DATE"),
            bigquery.SchemaField("to","DATE"),
            bigquery.SchemaField("symbol","STRING"),
            bigquery.SchemaField("headline","STRING"),
            bigquery.SchemaField("sentiment_score","FLOAT"),
        ]
    )

    job=client.load_table_from_dataframe(df,task_id,job_config=job_config)
    job.result()

    print(f"Loaded{job.output_rows} rows into {table_id}")




with DAG("Data_Orchestration_Pipeline", ##DAG name
         default_args=default_args,
         schedule_interval="10 6 * * *", ##schedule the orchestration pipeline everyday morning at 6:10AM
         catchup=False
) as dag:


    ##define the dags
    start=DummyOperator(task_id='start',dag=dag)

    

    ##load the news data from gcs bucket
    load_stock_news_from_gcs=PythonOperator(
        task_id="load_stock_news_from_gcs",
        python_callable=load_stock_news_from_gcs,
        dag=dag
    )

    ##news sentiment analysis 
    vertex_ai_sentiment=PythonOperator(
        task_id="vertex_ai_sentiment",
        python_callable=perform_sentiment_analysis,
        dag=dag
    )

    ##sentiment data laod to bigquery table
    store_to_bigquery=PythonOperator(
        task_id="store_to_bigquery",
        python_callable=load_to_bigquery,
        dag=dag
        )

    end=DummyOperator(task_id='end',dag=dag) ##end the orchestration pipeline


##define the DAG workflow
start>>load_stock_news_from_gcs>>vertex_ai_sentiment>>store_to_bigquery>>end










