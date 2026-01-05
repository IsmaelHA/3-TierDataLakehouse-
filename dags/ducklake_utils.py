import duckdb
import re
import datetime
from airflow.hooks.base import BaseHook
import os
# Ruta local para los datos de DuckLake
DUCKLAKE_DATA_PATH = "s3://transportationproject/ducklake/"
REGION='eu-north-1'
DUCKLAKE_ATTACH_NAME = "mobility_ducklake"
BRONZE_MITMA_TABLE='bronze_mobility_trips'
SILVER_MITMA_TABLE='silver_mobility_trips'
GOLD_MITMA_TABLE='gold_typical_day_patterns'
def connect_ducklake():
    """
    Conecta a DuckLake usando PostgreSQL (Neon) como catálogo de metadatos
    y almacenamiento local para los datos.
    
    Requiere variable de entorno en .env:
    AIRFLOW_CONN_NEON_POSTGRES=postgresql://user:pass@host:5432/db?sslmode=require
    """
    con = duckdb.connect()
    
    # Instalar y cargar extensiones
    con.execute("INSTALL ducklake;")
    con.execute("LOAD ducklake;")
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("INSTALL aws;")
    con.execute("LOAD aws;")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    
    # Obtener credenciales de Airflow
    pg_conn = BaseHook.get_connection('neon_postgres')
    
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if not aws_access_key:
        print("❌ CRITICAL: AWS_ACCESS_KEY_ID is None / Missing!")
    else:
        print(f"✅ Access Key found. Length: {len(aws_access_key)}")
        print(f"   First char: '{aws_access_key[0]}', Last char: '{aws_access_key[-1]}'")
        if len(aws_access_key.strip()) != len(aws_access_key):
            print("❌ WARNING: Access Key has hidden spaces!")
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_s3 (
            TYPE S3,
            PROVIDER config,
            KEY_ID '{aws_access_key}',
            SECRET '{aws_secret_key}',
            REGION 'eu-north-1',
            ENDPOINT 's3.eu-north-1.amazonaws.com'
        );
    """)

    # Crear secret para PostgreSQL
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE postgres,
            HOST '{pg_conn.host}',
            PORT {pg_conn.port or 5432},
            DATABASE '{pg_conn.schema}',
            USER '{pg_conn.login}',
            PASSWORD '{pg_conn.password}'
        )
    """)
    
    # Crear secret para DuckLake con PostgreSQL como catálogo
    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
        );
    """)
    
    # Attach DuckLake con datos locales
    con.execute(f"""
        ATTACH 'ducklake:secreto_ducklake' AS {DUCKLAKE_ATTACH_NAME} 
        (DATA_PATH '{DUCKLAKE_DATA_PATH}')
    """)
    
    con.execute(f"USE {DUCKLAKE_ATTACH_NAME}")
    
    return con

def close_ducklake(con):
    """Cierra la conexión a DuckLake de forma segura."""
    try:
        con.execute("USE memory")
        con.execute(f"DETACH {DUCKLAKE_ATTACH_NAME}")
    except:
        pass
    con.close()

def table_exists(con, table_name: str) -> bool:
    """Verifica si una tabla existe en el esquema actual."""
    result = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """).fetchone()[0]
    return result > 0

def extract_date_from_url(url):
    """Extrae la fecha de una URL con formato YYYYMMDD_Viajes_distritos."""
    match = re.search(r'/(\d{8})_Viajes_distritos', url)
    if match:
        return datetime.datetime.strptime(match.group(1), '%Y%m%d').date()
    return None
