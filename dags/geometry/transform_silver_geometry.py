def transform_silver_geometry(year: int):
    from ducklake_utils import connect_ducklake, close_ducklake
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        
        bronze_table = f'bronze_geometry_{year}'
        silver_table = 'silver_geometry_wgs84'
        
        # Skip if year already loaded
        count = con.execute(f"SELECT COUNT(*) FROM {silver_table} WHERE year = {year}").fetchone()[0]
        if count > 0:
            return
        
        con.execute(f"""
            INSERT INTO {silver_table}
            SELECT 
                ST_Transform(geom, 'EPSG:25830', 'EPSG:4326', true) AS geometry,
                CUSEC AS census_section_id,
                CUDIS AS district_id,
                CUMUN AS municipality_id,
                CPRO AS province_id,
                CCA AS autonomous_community_id,
                ST_Centroid(ST_Transform(geom, 'EPSG:25830', 'EPSG:4326')) AS centroid,
                {year} AS year
            FROM {bronze_table}
        """)
    finally:
        if con:
            close_ducklake(con)