from ducklake_utils import connect_ducklake, close_ducklake


def create_infrastructure_ranking():
    """Crea el ranking de infraestructura por municipio."""
    con = None
    try:
        con = connect_ducklake()
        
        con.execute("""
            CREATE OR REPLACE TABLE gold_municipality_infrastructure_ranking AS
            SELECT 
                origin_municipality,
                AVG(mismatch_ratio) AS avg_mismatch_ratio,
                MAX(origin_population) AS population,
                COUNT(*) AS num_connections,
                SUM(actual_mean_trips) AS total_actual_trips,
                SUM(predicted_trips) AS total_predicted_trips,
                CASE 
                    WHEN AVG(mismatch_ratio) < 0.5 THEN 'Well-served'
                    WHEN AVG(mismatch_ratio) BETWEEN 0.5 AND 1.5 THEN 'Adequately-served'
                    ELSE 'Underserved'
                END AS infrastructure_status
            FROM gold_gravity_model_analysis
            WHERE mismatch_ratio IS NOT NULL
            GROUP BY origin_municipality
            ORDER BY avg_mismatch_ratio DESC
        """)
        
        summary = con.execute("""
            SELECT 
                infrastructure_status,
                COUNT(*) AS municipios,
                SUM(population) AS poblacion_total
            FROM gold_municipality_infrastructure_ranking
            GROUP BY infrastructure_status
        """).fetchdf()
        
        print("✓ Ranking creado:")
        print(summary.to_string(index=False))
        
    finally:
        if con:
            close_ducklake(con)
