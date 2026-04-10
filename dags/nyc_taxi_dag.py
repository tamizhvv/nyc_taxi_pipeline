import datetime
import sys
sys.path.insert(0,'/home/tamiz/nyc_taxi_pipeline/scripts')
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract
from transform import transform
from load import load
from upload_to_s3 import upload_to_s3
default_args={
    'retries':2,
    'retry_delay':datetime.timedelta(minutes=1)
}

def run_pipeline():
    raw = extract()
    if raw is None:
        raise Exception("Extraction failed")
    cleaned = transform(raw)
    upload_to_s3(cleaned)
    load(cleaned)

with DAG (
    dag_id='nyc_taxi_pipeline',
    start_date=datetime.datetime(2026,4,10),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:
    pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline
    )