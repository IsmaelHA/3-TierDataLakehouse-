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
        query = "COPY (     SELECT table_name, column_name, data_type     FROM duckdb_columns() ) TO 'database_structure.csv' (HEADER, DELIMITER ',');"
        
        results = con.execute(query).fetchall()
        
        print("--- DAG Task Complete: Schema Extracted ---")
        for row in results:
            print(row[0]) # Print the CREATE TABLE statement
    finally:
        if con:
            close_ducklake(con)
# Run the 'DAG'
get_schema_task()