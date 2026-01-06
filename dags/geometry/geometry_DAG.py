from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime

from geometry.fetch_url_geometry import download_secciones_censales
from geometry.ingestion_bronze_geometry import ingestion_bronze_geometry
from geometry.create_silver_geometry import create_silver_geometry
from geometry.transform_silver_geometry import transform_silver_geometry

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='geometry_pipeline',
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
            description="Año de las secciones censales a procesar"
        ),
    }
) as dag:
    
    def fetch_task(**context):
        year = context['params']['year']
        return download_secciones_censales(year)
    
    def bronze_task(**context):
        year = context['params']['year']
        file_path = context['ti'].xcom_pull(task_ids='fetch')
        return ingestion_bronze_geometry(file_path, year)
    
    def silver_task(**context):
        year = context['params']['year']
        return transform_silver_geometry(year)
    
    create_silver = PythonOperator(
        task_id='create_silver_table',
        python_callable=create_silver_geometry
    )
    
    fetch = PythonOperator(
        task_id='fetch',
        python_callable=fetch_task
    )
    
    bronze = PythonOperator(
        task_id='bronze',
        python_callable=bronze_task
    )
    
    silver = PythonOperator(
        task_id='silver',
        python_callable=silver_task
    )
    
    fetch >> bronze
    [bronze, create_silver] >> silver
