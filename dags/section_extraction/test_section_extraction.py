"""
DAG para extraer las secciones censales de Valencia desde silver_geometry_wgs84
y crear la tabla gold_geometry_wgs84.

Coloca este archivo en: /dags/dag_valencia_extraction.py
Coloca valencia.shp en: /include/valencia.shp
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from ducklake_utils import connect_ducklake, close_ducklake
from section_extraction.extract_sections_from_polygon import create_gold_geometry_table


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

VALENCIA_SHAPEFILE = "/usr/local/airflow/include/valencia.shp"


def verify_silver_table(**context):
    """Verifica que existe silver_geometry_wgs84 y cuenta registros."""
    con = connect_ducklake()
    
    try:
        count = con.execute(
            "SELECT COUNT(*) FROM silver_geometry_wgs84"
        ).fetchone()[0]
        
        print(f"✓ Registros en silver_geometry_wgs84: {count:,}")
        context["ti"].xcom_push(key="silver_count", value=count)
        
        if count == 0:
            raise ValueError("La tabla silver_geometry_wgs84 está vacía")
            
    finally:
        close_ducklake(con)


def create_gold_table(**context):
    """Crea gold_geometry_wgs84 filtrando por el polígono de Valencia."""
    con = connect_ducklake()
    
    try:
        count = create_gold_geometry_table(
            con=con,
            shapefile_path=VALENCIA_SHAPEFILE,
            spatial_predicate="intersects"
        )
        
        print(f"✓ Registros en gold_geometry_wgs84: {count:,}")
        context["ti"].xcom_push(key="gold_count", value=count)
        
    finally:
        close_ducklake(con)


def verify_gold_table(**context):
    """Verifica la tabla gold y muestra las primeras filas."""
    import geopandas as gpd
    
    con = connect_ducklake()
    
    try:
        sample = con.execute("""
    SELECT ST_AsText(geometry) as wkt
    FROM silver_geometry_wgs84
    WHERE municipality_id = '46250'
    LIMIT 1
    """).fetchone()[0]
        print(f"Sample geometry silver: {sample[:100]}")
        # Debug: Comparar bounds
        gdf = gpd.read_file(VALENCIA_SHAPEFILE)
        print(f"CRS shapefile: {gdf.crs}")
        print(f"Bounds shapefile: {gdf.total_bounds}")
        
        bounds = con.execute("""
            SELECT 
                MIN(ST_XMin(geometry)) as min_x,
                MAX(ST_XMax(geometry)) as max_x,
                MIN(ST_YMin(geometry)) as min_y,
                MAX(ST_YMax(geometry)) as max_y
            FROM silver_geometry_wgs84
            WHERE municipality_id = '46250'
        """).fetchdf()
        print(f"Bounds silver Valencia:")
        print(bounds)
        print("\n" + "="*80)
        print("PRIMERAS 10 FILAS DE gold_geometry_wgs84:")
        print("="*80)
        
        df = con.execute("""
            SELECT 
                census_section_id,
                district_id,
                municipality_id,
                province_id,
                autonomous_community_id,
                year,
                ST_AsText(ST_Centroid(geometry)) as centroid_wkt
            FROM gold_geometry_wgs84
            LIMIT 10
        """).fetchdf()
        
        print(df.to_string(index=False))
        
        print("\n" + "="*80)
        print("RESUMEN POR DISTRITO:")
        print("="*80)
        
        df_districts = con.execute("""
            SELECT 
                district_id,
                COUNT(*) as num_sections
            FROM gold_geometry_wgs84
            GROUP BY district_id
            ORDER BY district_id
        """).fetchdf()
        
        print(df_districts.to_string(index=False))
        
        # Fix: obtener counts directamente si xcom falla
        silver_count = context["ti"].xcom_pull(key="silver_count")
        gold_count = context["ti"].xcom_pull(key="gold_count")
        
        if silver_count is None:
            silver_count = con.execute("SELECT COUNT(*) FROM silver_geometry_wgs84").fetchone()[0]
        if gold_count is None:
            gold_count = con.execute("SELECT COUNT(*) FROM gold_geometry_wgs84").fetchone()[0]
        
        print("\n" + "="*80)
        print("RESUMEN:")
        print("="*80)
        print(f"  Silver (total España): {silver_count:,}")
        print(f"  Gold (Valencia):       {gold_count:,}")
        print(f"  Porcentaje filtrado:   {gold_count/silver_count*100:.2f}%")
        
    finally:
        close_ducklake(con)


# Definición del DAG
with DAG(
    dag_id="valencia_geometry_extraction",
    default_args=default_args,
    description="Extrae secciones censales de Valencia a gold_geometry_wgs84",
    schedule=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["geometry", "valencia", "gold"],
) as dag:
    
    task_verify_silver = PythonOperator(
        task_id="verify_silver_table",
        python_callable=verify_silver_table,
    )
    
    task_create_gold = PythonOperator(
        task_id="create_gold_table",
        python_callable=create_gold_table,
    )
    
    task_verify_gold = PythonOperator(
        task_id="verify_gold_table",
        python_callable=verify_gold_table,
    )
    
    # Pipeline
    task_verify_silver >> task_create_gold >> task_verify_gold