"""
DAG integrado: Extracción de geometría + Modelo de gravedad

1. Extrae secciones censales de un polígono WKT
2. Ejecuta el modelo de gravedad sobre esa zona
3. Genera mapa de infraestructura

Calcula: T_ij = k * (P_i * E_j) / d_ij^2

Tablas creadas:
- gold_geometry_wgs84: Geometrías filtradas por el polígono
- gold_gravity_model_analysis: Análisis completo del modelo
- gold_municipality_infrastructure_ranking: Ranking de infraestructura

Outputs:
- infrastructure_map.html: Mapa Kepler con zonas well-served/underserved
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="gravity_model_pipeline",
    default_args=default_args,
    description="Extracción de geometría + Modelo de gravedad + Mapa",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["gravity", "model", "gold", "geometry"],
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
    }
) as dag:
    
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
    
    # Tareas
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
    
    # Flujo
    t_extract >> t_verify >> t_centroids >> t_distances >> [t_trips, t_economy] >> t_gravity >> t_gold >> t_ranking >> t_map >> t_cleanup
