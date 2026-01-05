from ducklake_utils import connect_ducklake, close_ducklake


def aggregate_economy(year: int = 2023):
    """Agrega datos económicos a nivel de municipio."""
    con = None
    try:
        con = connect_ducklake()
        
        con.execute(f"""
            CREATE OR REPLACE TABLE temp_economy_by_municipality AS
            SELECT 
                municipality_code,
                AVG(avg_income) AS avg_income
            FROM silver_economy_aggregated
            WHERE year = {year}
            GROUP BY municipality_code
        """)
        
        count = con.execute("SELECT COUNT(*) FROM temp_economy_by_municipality").fetchone()[0]
        print(f"✓ Municipios con datos económicos ({year}): {count}")
        
    finally:
        if con:
            close_ducklake(con)
