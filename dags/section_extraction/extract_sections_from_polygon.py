import duckdb
import geopandas as gpd


def filter_geometry_by_shapefile(
    con: duckdb.DuckDBPyConnection,
    shapefile_path: str,
    spatial_predicate: str = "intersects"
) -> duckdb.DuckDBPyRelation:
    """
    Filtra silver_geometry_wgs84 usando un polígono de un shapefile.
    
    Args:
        con: Conexión a DuckDB con extensión spatial cargada
        shapefile_path: Ruta al archivo shapefile (.shp)
        spatial_predicate: Tipo de filtro espacial ('intersects', 'within', 'contains')
    
    Returns:
        Relación DuckDB con los registros filtrados (gold_geometry_wgs84)
    """
    # Leer el shapefile y obtener la geometría unificada
    gdf = gpd.read_file(shapefile_path)
    
    # Asegurar que está en WGS84 (EPSG:4326)
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    
    # Unir todas las geometrías en una sola (por si hay múltiples polígonos)
    filter_polygon = gdf.unary_union.wkt
    
    # Mapear predicados espaciales
    predicates = {
        "intersects": "ST_Intersects",
        "within": "ST_Within",
        "contains": "ST_Contains",
        "overlaps": "ST_Overlaps"
    }
    
    st_function = predicates.get(spatial_predicate, "ST_Intersects")
    
    # Ejecutar query espacial
    query = f"""
        SELECT 
            geometry,
            census_section_id,
            district_id,
            municipality_id,
            province_id,
            autonomous_community_id,
            centroid,
            year
        FROM silver_geometry_wgs84
        WHERE {st_function}(
            geometry, 
            ST_GeomFromText('{filter_polygon}')
        )
    """
    
    return con.execute(query)


def create_gold_geometry_table(
    con: duckdb.DuckDBPyConnection,
    shapefile_path: str,
    spatial_predicate: str = "intersects"
) -> int:
    gdf = gpd.read_file(shapefile_path)
    
    # Forzar reproyección a WGS84
    gdf = gdf.to_crs(epsg=4326)
    print(f"CRS reproyectado: {gdf.crs}")
    print(f"Bounds reproyectados: {gdf.total_bounds}")
    
    filter_polygon = gdf.unary_union.wkt
    print(f"WKT (primeros 200 chars): {filter_polygon[:200]}")
    
    predicates = {
        "intersects": "ST_Intersects",
        "within": "ST_Within",
        "contains": "ST_Contains"
    }
    st_function = predicates.get(spatial_predicate, "ST_Intersects")
    
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_geometry_wgs84 AS
        SELECT *
        FROM silver_geometry_wgs84
        WHERE {st_function}(
            geometry, 
            ST_GeomFromText('{filter_polygon}')
        )
    """)
    
    count = con.execute("SELECT COUNT(*) FROM gold_geometry_wgs84").fetchone()[0]
    return count