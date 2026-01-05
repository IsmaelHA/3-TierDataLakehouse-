from ducklake_utils import connect_ducklake, close_ducklake


def cleanup_temp_tables():
    """Limpia las tablas temporales."""
    con = None
    try:
        con = connect_ducklake()
        
        for table in ["temp_municipality_centroids", "temp_municipality_distances", 
                      "temp_trips_by_municipality", "temp_economy_by_municipality", 
                      "temp_gravity_data"]:
            con.execute(f"DROP TABLE IF EXISTS {table}")
        
        print("✓ Tablas temporales eliminadas")
        
    finally:
        if con:
            close_ducklake(con)
