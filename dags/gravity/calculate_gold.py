from ducklake_utils import connect_ducklake, close_ducklake


def calculate_and_create_gold():
    """Calcula k y crea la tabla gold."""
    con = None
    try:
        con = connect_ducklake()
        
        # Calcular constante k
        k_result = con.execute("""
            SELECT 
                SUM(actual_mean_trips * distance_km * distance_km) / 
                NULLIF(SUM(origin_population * dest_economic_activity), 0) AS k_factor
            FROM temp_gravity_data
            WHERE actual_mean_trips IS NOT NULL
        """).fetchone()
        
        k_factor = k_result[0] if k_result[0] else 1.0
        print(f"✓ Constante de calibración k = {k_factor:.10f}")
        
        # Crear tabla gold
        con.execute(f"""
            CREATE OR REPLACE TABLE gold_gravity_model_analysis AS
            SELECT 
                g.origin_municipality,
                g.dest_municipality,
                g.distance_km,
                g.origin_population,
                g.dest_economic_activity,
                g.actual_mean_trips,
                g.std_trips,
                {k_factor} * (g.origin_population * g.dest_economic_activity) / 
                    (g.distance_km * g.distance_km) AS predicted_trips,
                CASE 
                    WHEN {k_factor} * (g.origin_population * g.dest_economic_activity) / 
                         (g.distance_km * g.distance_km) > 0 
                    THEN g.actual_mean_trips / ({k_factor} * (g.origin_population * g.dest_economic_activity) / 
                         (g.distance_km * g.distance_km))
                    ELSE NULL 
                END AS mismatch_ratio,
                {k_factor} AS calibration_constant,
                CURRENT_TIMESTAMP AS created_at
            FROM temp_gravity_data g
        """)
        
        count = con.execute("SELECT COUNT(*) FROM gold_gravity_model_analysis").fetchone()[0]
        print(f"✓ gold_gravity_model_analysis: {count:,} filas")
        
    finally:
        if con:
            close_ducklake(con)
