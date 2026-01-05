from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from geometry.geometry.fetch_url_geometry import download_secciones_censales
from geometry.geometry.ingestion_bronze_geometry import ingestion_bronze_geometry
from geometry.geometry.create_silver_geometry import create_silver_geometry
from geometry.geometry.transform_silver_geometry import transform_silver_geometry

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
    max_active_tasks=1
) as dag:
    
    create_silver = PythonOperator(
        task_id='create_silver_table',
        python_callable=create_silver_geometry
    )
    
    years = [2022, 2023]
    
    for year in years:
        fetch = PythonOperator(
            task_id=f'fetch_{year}',
            python_callable=download_secciones_censales,
            op_kwargs={'year': year}
        )
        
        bronze = PythonOperator(
            task_id=f'bronze_{year}',
            python_callable=lambda y=year, **ctx: ingestion_bronze_geometry(
                ctx['ti'].xcom_pull(task_ids=f'fetch_{y}'), y
            )
        )
        
        silver = PythonOperator(
            task_id=f'silver_{year}',
            python_callable=transform_silver_geometry,
            op_kwargs={'year': year}
        )
        
        fetch >> bronze
        [bronze, create_silver] >> silver