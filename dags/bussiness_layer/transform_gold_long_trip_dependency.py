def transform_gold_long_trip_dependency():
    from ducklake_utils import connect_ducklake, close_ducklake
    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")

        con.execute("""
            CREATE OR REPLACE TABLE gold_long_trip_dependency AS
            WITH trips AS (
              SELECT
                origin_zone_id,
                destination_zone_id,
                trip_timestamp,
                total_trips
              FROM silver_mobility_trips
            ),
            geo AS (
              SELECT
                municipality_id AS zone_id,
                centroid
              FROM silver_geometry_wgs84
            ),
            joined AS (
              SELECT
                t.origin_zone_id,
                t.destination_zone_id,
                t.total_trips,
                EXTRACT(year FROM t.trip_timestamp) AS year,
                ST_Distance_Spheroid(g1.centroid, g2.centroid) / 1000.0 AS distance_km
              FROM trips t
              JOIN geo g1 ON CAST(t.origin_zone_id AS VARCHAR) = CAST(g1.zone_id AS VARCHAR)
              JOIN geo g2 ON CAST(t.destination_zone_id AS VARCHAR) = CAST(g2.zone_id AS VARCHAR)
            ),
            agg AS (
              SELECT
                year,
                origin_zone_id,
                SUM(total_trips) AS total_trips,
                SUM(CASE WHEN distance_km > 15 THEN total_trips ELSE 0 END) AS long_trips,
                AVG(distance_km) AS avg_trip_km
              FROM joined
              WHERE distance_km IS NOT NULL
              GROUP BY year, origin_zone_id
            )
            SELECT
              year,
              origin_zone_id AS municipality_id,
              total_trips,
              long_trips,
              CASE WHEN total_trips = 0 THEN NULL ELSE long_trips * 1.0 / total_trips END AS long_trip_ratio,
              avg_trip_km
            FROM agg
        """)
    finally:
        if con:
            close_ducklake(con)
