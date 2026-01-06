"""
DAG Unificado: Business Questions Analysis

Combina las tres preguntas de negocio:
1. Typical Day Patterns (generate_report) - Patrones de movilidad típicos
2. Gravity Model (gravity) - Modelo de gravedad para infraestructura
3. Long Trip Dependency - Dependencia de viajes largos (falta de coches)

Outputs:
- PDF Report + CSV en S3
- infrastructure_map.html (Kepler)
- gold_long_trip_dependency table
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup

# Imports de gravity
from gravity.extract_geometry import extract_geometry, DEFAULT_WKT
from gravity.verify_dependencies import verify_dependencies
from gravity.create_centroids import create_municipality_centroids
from gravity.create_distances import create_municipality_distances
from gravity.aggregate_trips import aggregate_trips
from gravity.aggregate_economy import aggregate_economy
from gravity.create_gravity_data import create_gravity_data
from gravity.calculate_gold import calculate_and_create_gold
from gravity.create_ranking import create_infrastructure_ranking
from gravity.create_map import create_infrastructure_map
from gravity.cleanup import cleanup_temp_tables

# Import de long trip dependency
from bussiness_layer.transform_gold_long_trip_dependency import transform_gold_long_trip_dependency

# Import de generate report
from bussiness_layer.generate_report import generate_mobility_report_s3

from ducklake_utils import connect_ducklake, close_ducklake


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Configuración S3
S3_BUCKET = "transportationproject"


with DAG(
    dag_id="business_questions_pipeline",
    default_args=default_args,
    description="Pipeline unificado: Typical Patterns + Gravity Model + Long Trip Dependency",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["business", "gold", "analysis", "gravity", "report"],
    params={
        "wkt_polygon": Param(
            default=DEFAULT_WKT,
            type="string",
            description="Polígono WKT en EPSG:4326 (WGS84)"
        ),
        "spatial_predicate": Param(
            default="intersects",
            type="string",
            enum=["intersects", "contains", "within"],
            description="Predicado espacial"
        ),
        "year": Param(
            default=2023,
            type="integer",
            minimum=2020,
            maximum=2025,
            description="Año para datos de población y economía"
        ),
        "target_origins": Param(
            default=["4600501","4600502","4600503"],
            type="array",
            description="Lista de zonas origen para el reporte (ej: códigos de municipio)"
        ),
    }
) as dag:
    
    # =========================================================================
    # TASK GROUP 1: TYPICAL DAY PATTERNS (Business Question 1)
    # =========================================================================
    with TaskGroup("bq1_typical_patterns", tooltip="Business Question 1: Typical Day Patterns") as bq1_group:
        
        def _generate_report(**context):
            target_origins = context['params'].get('target_origins', ["4600501","4600502","4600503"])
            con = None
            try:
                con = connect_ducklake()
                generate_mobility_report_s3(
                    con=con,
                    target_origins=target_origins,
                    bucket_name=S3_BUCKET,
                    s3_key="ducklake/reports/day_profiles.pdf"
                )
            finally:
                if con:
                    close_ducklake(con)
        
        t_report = PythonOperator(
            task_id="generate_mobility_report",
            python_callable=_generate_report
        )
    
    # =========================================================================
    # TASK GROUP 2: GRAVITY MODEL (Business Question 2)
    # =========================================================================
    with TaskGroup("bq2_gravity_model", tooltip="Business Question 2: Gravity Model") as bq2_group:
        
        def _extract_geometry(**context):
            wkt = context['params'].get('wkt_polygon', DEFAULT_WKT)
            predicate = context['params'].get('spatial_predicate', 'intersects')
            extract_geometry(wkt, predicate)
        
        def _aggregate_economy(**context):
            year = context['params'].get('year', 2023)
            aggregate_economy(year)
        
        def _create_gravity_data(**context):
            year = context['params'].get('year', 2023)
            create_gravity_data(year)
        
        t_extract = PythonOperator(task_id="extract_geometry", python_callable=_extract_geometry)
        t_verify = PythonOperator(task_id="verify_dependencies", python_callable=verify_dependencies)
        t_centroids = PythonOperator(task_id="create_centroids", python_callable=create_municipality_centroids)
        t_distances = PythonOperator(task_id="create_distances", python_callable=create_municipality_distances)
        t_trips = PythonOperator(task_id="aggregate_trips", python_callable=aggregate_trips)
        t_economy = PythonOperator(task_id="aggregate_economy", python_callable=_aggregate_economy)
        t_gravity = PythonOperator(task_id="create_gravity_data", python_callable=_create_gravity_data)
        t_gold = PythonOperator(task_id="create_gold", python_callable=calculate_and_create_gold)
        t_ranking = PythonOperator(task_id="create_ranking", python_callable=create_infrastructure_ranking)
        t_map = PythonOperator(task_id="create_map", python_callable=create_infrastructure_map)
        t_cleanup = PythonOperator(task_id="cleanup", python_callable=cleanup_temp_tables)
        
        # Flujo interno del grupo
        t_extract >> t_verify >> t_centroids >> t_distances >> [t_trips, t_economy] >> t_gravity >> t_gold >> t_ranking >> t_map >> t_cleanup
    
    # =========================================================================
    # TASK GROUP 3: LONG TRIP DEPENDENCY (Business Question 3)
    # =========================================================================
    with TaskGroup("bq3_long_trip_dependency", tooltip="Business Question 3: Long Trip Dependency") as bq3_group:
        
        t_long_trip = PythonOperator(
            task_id="create_long_trip_dependency",
            python_callable=transform_gold_long_trip_dependency
        )
    
    # =========================================================================
    # FLUJO PRINCIPAL: Las 3 Business Questions en paralelo
    # =========================================================================
    [bq1_group, bq2_group, bq3_group]
