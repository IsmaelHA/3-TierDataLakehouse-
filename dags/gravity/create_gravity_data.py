from ducklake_utils import connect_ducklake, close_ducklake


def create_gravity_data(year: int = 2023):
    """Combina todos los datos para el modelo de gravedad."""
    con = None
    try:
        con = connect_ducklake()
        
        con.execute(f"""
            CREATE OR REPLACE TABLE temp_gravity_data AS
            SELECT 
                md.origin_municipality,
                md.dest_municipality,
                md.distance_km,
                COALESCE(po.population, 0) AS origin_population,
                COALESCE(ed.avg_income, 0) AS dest_economic_activity,
                tm.mean_trips AS actual_mean_trips,
                tm.std_trips
            FROM temp_municipality_distances md
            LEFT JOIN silver_population po 
                ON md.origin_municipality = po.municipality_code
                AND po.year = {year}
            LEFT JOIN temp_economy_by_municipality ed 
                ON md.dest_municipality = ed.municipality_code
            LEFT JOIN temp_trips_by_municipality tm 
                ON md.origin_municipality = tm.origin_municipality
                AND md.dest_municipality = tm.dest_municipality
            WHERE COALESCE(po.population, 0) > 0 
                AND COALESCE(ed.avg_income, 0) > 0
        """)
        
        count = con.execute("SELECT COUNT(*) FROM temp_gravity_data").fetchone()[0]
        print(f"✓ Pares con datos completos: {count:,}")
        
    finally:
        if con:
            close_ducklake(con)
