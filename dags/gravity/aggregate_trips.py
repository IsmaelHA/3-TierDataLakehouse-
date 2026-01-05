from ducklake_utils import connect_ducklake, close_ducklake


def aggregate_trips():
    """Agrega viajes a nivel de municipio."""
    con = None
    try:
        con = connect_ducklake()
        
        con.execute("""
            CREATE OR REPLACE TABLE temp_trips_by_municipality AS
            SELECT 
                LEFT(origin_zone, 5) AS origin_municipality,
                LEFT(destination_zone, 5) AS dest_municipality,
                SUM(avg_trips) AS mean_trips,
                AVG(std_trips) AS std_trips
            FROM gold_typical_day_patterns
            GROUP BY LEFT(origin_zone, 5), LEFT(destination_zone, 5)
        """)
        
        count = con.execute("SELECT COUNT(*) FROM temp_trips_by_municipality").fetchone()[0]
        print(f"✓ Pares origen-destino: {count:,}")
        
    finally:
        if con:
            close_ducklake(con)
