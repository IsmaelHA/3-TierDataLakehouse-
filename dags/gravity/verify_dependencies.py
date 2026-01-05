from ducklake_utils import connect_ducklake, close_ducklake, table_exists


def verify_dependencies():
    """Verifica que existen las tablas necesarias para el modelo."""
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        required_tables = [
            "gold_geometry_wgs84",
            "gold_typical_day_patterns", 
            "silver_population",
            "silver_economy_aggregated"
        ]
        
        missing = []
        for table in required_tables:
            exists = table_exists(con, table)
            status = "✓" if exists else "✗"
            print(f"  {status} {table}")
            if not exists:
                missing.append(table)
        
        if missing:
            raise ValueError(f"Tablas faltantes: {', '.join(missing)}")
        
        print("\n✓ Todas las dependencias verificadas")
        
    finally:
        if con:
            close_ducklake(con)
