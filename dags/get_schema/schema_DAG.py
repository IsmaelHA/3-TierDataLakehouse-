from datetime import datetime, timedelta
import duckdb
from get_schema.get_shema import get_schema_task
from get_schema.ducklake_utils import connect_ducklake, close_ducklake

# --- Use your preferred imports ---
# Note: If airflow.sdk is not available in your environment, 
# switch to: from airflow.decorators import dag, task
from airflow.sdk import dag, task, Param

# --- Default Arguments ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

@dag(
    dag_id='duckdb_schema_inspector',
    default_args=default_args,
    description='Extracts and logs the Create Table (DDL) statements from a DuckDB file',
    schedule=None,  # Set to None for manual triggering only
    catchup=False,
    tags=['duckdb', 'schema', 'ops'],
    # Allow the user to specify the DB path when triggering the DAG
)
def duckdb_schema_pipeline():

    # 1. TASK: Get Schema DDL
    @task
    def task_get_schema_ddl(**context):

        # 2. Connect to DuckDB
        # We use read_only=True to prevent accidental writes during inspection
        try:
            con = connect_ducklake
        except Exception as e:
            # Fallback if the file doesn't exist or permissions fail
            print(f"Error connecting: {e}. Falling back to in-memory for demo.")
            close_ducklake(con)

        # 3. Run the specific query you asked for
        # This retrieves the 'CREATE TABLE' statements
        query = "SELECT sql FROM sqlite_master WHERE type = 'table';"
        results = con.execute(query).fetchall()

        # 4. Print the output to the Airflow Task Logs
        print(f"--- Found {len(results)} tables. Schema Definitions below: ---")
        
        schema_statements = []
        for i, row in enumerate(results):
            ddl = row[0]
            # Print clearly to logs
            print(f"\n[Table {i+1}]:\n{ddl}")
            schema_statements.append(ddl)
            
        close_ducklake(con)
        
        # Return the list of DDL strings (viewable in XComs)
        return schema_statements

    # Call the task
    task_get_schema_ddl()

# Instantiate the DAG
duckdb_schema_pipeline()