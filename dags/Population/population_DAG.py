from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from Population.fetch_url_population import download_population_csv
from Population.ingestion_bronze_population import ingestion_bronze_population
from Population.transform_silver_population import transform_silver_population

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="population_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
) as dag:

    # No pedimos year por params.
    # Ingerimos SOURCE_YEAR pero lo materializamos como TARGET_YEAR.
    TARGET_YEAR = 2023
    SOURCE_YEAR = 2022

    def fetch_task(**context):
        # El endpoint del INE devuelve el dataset completo; el year solo se usa para el nombre cacheado.
        return download_population_csv(TARGET_YEAR)

    def bronze_task(**context):
        file_path = context["ti"].xcom_pull(task_ids="fetch")
        # Crea bronze_population_<TARGET_YEAR> con filas del SOURCE_YEAR
        return ingestion_bronze_population(
            file_path,
            target_year=TARGET_YEAR,
            source_year=SOURCE_YEAR,
            overwrite=True,
        )

    def silver_task(**context):
        # Escribe en silver como TARGET_YEAR aunque el source sea SOURCE_YEAR
        return transform_silver_population(TARGET_YEAR, force_year=TARGET_YEAR)

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

