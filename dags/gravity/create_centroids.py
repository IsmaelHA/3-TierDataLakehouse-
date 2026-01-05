from ducklake_utils import connect_ducklake, close_ducklake


def create_municipality_centroids():
    """Agrega centroides a nivel de municipio."""
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        con.execute("""
            CREATE OR REPLACE TABLE temp_municipality_centroids AS
            SELECT 
                municipality_id AS municipality_code,
                ST_Centroid(ST_Union_Agg(centroid)) AS centroid
            FROM gold_geometry_wgs84
            GROUP BY municipality_id
        """)
        
        count = con.execute("SELECT COUNT(*) FROM temp_municipality_centroids").fetchone()[0]
        print(f"✓ Centroides agregados: {count} municipios")
        
    finally:
        if con:
            close_ducklake(con)
