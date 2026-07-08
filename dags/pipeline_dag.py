import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from pipeline.download_csvs import download_business_owners, download_business_licenses
from pipeline.transform import transform_and_write_parquet
from pipeline.validation import validate_parquet_outputs
from pipeline.clean_up import delete_downloaded_csvs

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="pipeline_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz="America/New_York",
    ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    download_owners_task = PythonOperator(
        task_id="download_business_owners",
        python_callable=download_business_owners,
    )

    download_licenses_task = PythonOperator(
        task_id="download_business_licenses",
        python_callable=download_business_licenses,
    )

    transform_task = PythonOperator(
        task_id="transform_and_write_parquet",
        python_callable=transform_and_write_parquet,
        op_args=[
            download_owners_task.output,
            download_licenses_task.output,
        ],
    )

    validate_task = PythonOperator(
        task_id="validate_parquet_outputs",
        python_callable=validate_parquet_outputs,
        op_args=[
            transform_task.output,
        ],
    )

    cleanup_task = PythonOperator(
        task_id="delete_downloaded_csvs",
        python_callable=delete_downloaded_csvs,
        op_args=[
            download_owners_task.output,
            download_licenses_task.output,
        ],
    )

    [
        download_owners_task,
        download_licenses_task,
    ] >> transform_task >> validate_task >> cleanup_task