def create_silver_geometry():
    from ducklake_utils import connect_ducklake, close_ducklake
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver_geometry_wgs84 (
            geometry GEOMETRY,
            census_section_id VARCHAR,
            district_id VARCHAR,
            municipality_id VARCHAR,
            province_id VARCHAR,
            autonomous_community_id VARCHAR,
            centroid GEOMETRY,
            year INT
            );
        """)
    finally:
        if con:
            close_ducklake(con)