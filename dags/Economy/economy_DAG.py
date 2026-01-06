from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime

from Economy.fetch_url_economy import download_economy_csv
from Economy.ingestion_bronze_economy import ingestion_bronze_economy
from Economy.transform_silver_economy import transform_silver_economy

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="economy_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    params={
        "year": Param(
            default=2023,
            type="integer",
            minimum=2011,
            maximum=2030,
            description="Year to process"
        ),
    }
) as dag:

    def fetch_task(**context):
        year = context["params"]["year"]
        return download_economy_csv(year)

    def bronze_task(**context):
        year = context["params"]["year"]
        file_path = context["ti"].xcom_pull(task_ids="fetch")
        return ingestion_bronze_economy(file_path, year)

    def silver_task(**context):
        year = context["params"]["year"]
        return transform_silver_economy(year)

    fetch = PythonOperator(
        task_id="fetch",
        python_callable=fetch_task
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=bronze_task
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=silver_task
    )

    fetch >> bronze >> silver
