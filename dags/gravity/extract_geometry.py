from ducklake_utils import connect_ducklake, close_ducklake

DEFAULT_WKT = """POLYGON((-0.5663 39.5765, -0.2295 39.5765, -0.2295 39.3073, -0.5663 39.3073, -0.5663 39.5765))"""


def extract_geometry(wkt_polygon: str = DEFAULT_WKT, spatial_predicate: str = 'intersects'):
    """Extrae geometrías del polígono WKT a gold_geometry_wgs84."""
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        # Verificar silver
        silver_count = con.execute("SELECT COUNT(*) FROM silver_geometry_wgs84").fetchone()[0]
        print(f"✓ Registros en silver_geometry_wgs84: {silver_count:,}")
        
        if silver_count == 0:
            raise ValueError("La tabla silver_geometry_wgs84 está vacía")
        
        # Seleccionar predicado espacial
        if spatial_predicate == 'contains':
            predicate_sql = f"ST_Contains(ST_GeomFromText('{wkt_polygon}'), s.geometry)"
        elif spatial_predicate == 'within':
            predicate_sql = f"ST_Within(s.geometry, ST_GeomFromText('{wkt_polygon}'))"
        else:
            predicate_sql = f"ST_Intersects(s.geometry, ST_GeomFromText('{wkt_polygon}'))"
        
        # Crear gold_geometry_wgs84
        con.execute(f"""
            CREATE OR REPLACE TABLE gold_geometry_wgs84 AS
            SELECT 
                s.geometry,
                s.census_section_id,
                s.district_id,
                s.municipality_id,
                s.province_id,
                s.autonomous_community_id,
                s.centroid,
                s.year
            FROM silver_geometry_wgs84 s
            WHERE {predicate_sql}
        """)
        
        gold_count = con.execute("SELECT COUNT(*) FROM gold_geometry_wgs84").fetchone()[0]
        print(f"✓ Registros en gold_geometry_wgs84: {gold_count:,}")
        print(f"✓ Porcentaje filtrado: {gold_count/silver_count*100:.2f}%")
        
        # Resumen por municipio
        df = con.execute("""
            SELECT municipality_id, COUNT(*) as sections
            FROM gold_geometry_wgs84
            GROUP BY municipality_id
            ORDER BY sections DESC
        """).fetchdf()
        print(f"\n✓ Municipios extraídos: {len(df)}")
        
    finally:
        if con:
            close_ducklake(con)
