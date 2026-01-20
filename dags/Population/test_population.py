"""Test rapido de la tabla silver_population dentro de DuckLake (local).

Este script sustituye el path Windows hardcoded y consulta directamente la tabla.
"""


def test_silver_population(year: int = 2023):
    from ducklake_utils import connect_ducklake, close_ducklake

    con = None
    try:
        con = connect_ducklake()

        total = con.execute("SELECT COUNT(*) FROM silver_population").fetchone()[0]
        print(f"Total rows in silver_population: {total}")

        by_year = con.execute(
            "SELECT year, COUNT(*) AS count FROM silver_population GROUP BY year ORDER BY year"
        ).fetchall()
        print("\nRows by year:")
        for y, c in by_year:
            print(f"  {y}: {c}")

        sample = con.execute(
            "SELECT municipality_code, year, population FROM silver_population WHERE year = ? LIMIT 5",
            [year],
        ).fetchall()
        print("\nSample rows:")
        for row in sample:
            print(f"  {row}")

        nulls = con.execute(
            """
            SELECT COUNT(*)
            FROM silver_population
            WHERE municipality_code IS NULL OR year IS NULL OR population IS NULL
            """
        ).fetchone()[0]
        print(f"\nNULLs in key fields: {nulls}/{total}")

        negatives = con.execute(
            "SELECT COUNT(*) FROM silver_population WHERE population < 0"
        ).fetchone()[0]
        print(f"Negative population: {negatives}/{total}")

        duplicates = con.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT municipality_code, year, COUNT(*) AS n
                FROM silver_population
                GROUP BY municipality_code, year
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        print(f"Duplicate keys (municipality_code,year): {duplicates}")

    finally:
        if con:
            close_ducklake(con)


if __name__ == "__main__":
    test_silver_population(2023)
