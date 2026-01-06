import duckdb
from ducklake_utils import connect_ducklake, close_ducklake
def get_schema_task():
    """
    Task: Retrieve schema DDL from DuckDB
    """
    con=None
    try:
            
        con = connect_ducklake()
        
        # The Query
        query = "SELECT sql FROM sqlite_master WHERE type = 'table';"
        
        results = con.execute(query).fetchall()
        
        print("--- DAG Task Complete: Schema Extracted ---")
        for row in results:
            print(row[0]) # Print the CREATE TABLE statement
    finally:
        if con:
            close_ducklake(con)
# Run the 'DAG'
get_schema_task()