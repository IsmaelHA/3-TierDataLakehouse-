from ducklake_utils import connect_ducklake, close_ducklake
import os


def create_infrastructure_map():
    """Genera mapa HTML con Kepler.gl."""
    from keplergl import KeplerGl
    
    output_dir = "/usr/local/airflow/include/outputs"
    os.makedirs(output_dir, exist_ok=True)
    
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        df = con.execute("""
            SELECT 
                r.origin_municipality AS municipality_id,
                r.avg_mismatch_ratio,
                r.population,
                r.num_connections,
                r.total_actual_trips,
                r.total_predicted_trips,
                r.infrastructure_status,
                CASE 
                    WHEN r.infrastructure_status = 'Well-served' THEN 1
                    WHEN r.infrastructure_status = 'Adequately-served' THEN 2
                    ELSE 3
                END AS status_code,
                ST_AsGeoJSON(ST_Union_Agg(g.geometry)) AS geometry
            FROM gold_municipality_infrastructure_ranking r
            JOIN gold_geometry_wgs84 g 
                ON r.origin_municipality = g.municipality_id
            GROUP BY 
                r.origin_municipality,
                r.avg_mismatch_ratio,
                r.population,
                r.num_connections,
                r.total_actual_trips,
                r.total_predicted_trips,
                r.infrastructure_status
        """).fetchdf()
        
        print(f"✓ Municipios con geometría: {len(df)}")
        
        map_kepler = KeplerGl(height=700)
        map_kepler.add_data(data=df, name="infrastructure")
        
        output_path = f"{output_dir}/infrastructure_map.html"
        map_kepler.save_to_html(file_name=output_path)
        
        print(f"✓ Mapa guardado en: {output_path}")
        
    finally:
        if con:
            close_ducklake(con)
