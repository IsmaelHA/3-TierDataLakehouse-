from ducklake_utils import connect_ducklake, close_ducklake


def create_municipality_distances():
    """Calcula distancias entre municipios."""
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        con.execute("""
            CREATE OR REPLACE TABLE temp_municipality_distances AS
            SELECT 
                o.municipality_code AS origin_municipality,
                d.municipality_code AS dest_municipality,
                ST_Distance_Spheroid(o.centroid, d.centroid) / 1000.0 AS distance_km
            FROM temp_municipality_centroids o
            CROSS JOIN temp_municipality_centroids d
            WHERE o.municipality_code != d.municipality_code
                AND ST_Distance_Spheroid(o.centroid, d.centroid) / 1000.0 > 0.1
        """)
        
        count = con.execute("SELECT COUNT(*) FROM temp_municipality_distances").fetchone()[0]
        print(f"✓ Pares de distancias calculados: {count:,}")
        
    finally:
        if con:
            close_ducklake(con)
