import duckdb

def test_silver_geometry():
    con = None
    try:
        con = duckdb.connect()
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        
        parquet_path = "C:/Users/34666/Desktop/proyecto/include/my_ducklake.ducklake.files/main/silver_geometry_wgs84/*.parquet"
        
        total = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        print(f"Total rows: {total}")
        
        by_year = con.execute(f"""
            SELECT year, COUNT(*) as count 
            FROM '{parquet_path}'
            GROUP BY year 
            ORDER BY year
        """).fetchall()
        print("\nRows by year:")
        for year, count in by_year:
            print(f"  {year}: {count}")
        
        sample = con.execute(f"""
            SELECT 
                census_section_id,
                district_id,
                municipality_id,
                province_id,
                autonomous_community_id,
                year
            FROM '{parquet_path}'
            LIMIT 5
        """).fetchall()
        print("\nSample rows:")
        for row in sample:
            print(f"  {row}")
        
        valid = con.execute(f"""
            SELECT COUNT(*) 
            FROM '{parquet_path}'
            WHERE ST_IsValid(geometry)
        """).fetchone()[0]
        print(f"\nValid geometries: {valid}/{total}")
        
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    test_silver_geometry()