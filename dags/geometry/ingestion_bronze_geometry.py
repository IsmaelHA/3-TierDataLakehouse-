def ingestion_bronze_geometry(shp_path: str, year: int):
    from ducklake_utils import connect_ducklake, close_ducklake, table_exists
    con = None
    try:
        con = connect_ducklake()
        table_name = f'bronze_geometry_{year}'
        
        if table_exists(con, table_name):
            return
        
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        con.execute(f"""
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT 
        geom,
        CUSEC,
        CUDIS,
        CUMUN,
        CPRO,
        CCA,
        CURRENT_TIMESTAMP AS ingestion_date
    FROM ST_Read('{shp_path}')
        """)
    finally:
        if con:
            close_ducklake(con)