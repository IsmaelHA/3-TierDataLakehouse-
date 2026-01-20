def transform_gold_long_trip_dependency(year: int | None = None, generate_map: bool = True):
    """
    Business Question 3 (Correct):
    Long-distance trip dependency per origin municipality.
    - We keep trips whose ORIGIN is inside the study polygon (gold_geometry_wgs84).
    - DESTINATION can be outside the polygon (otherwise big cities get underestimated).
    - Distance is computed when both origin and destination centroids are available.
    - Optionally generates a Kepler.gl HTML map in /usr/local/airflow/include/outputs
    """

    import os
    from ducklake_utils import connect_ducklake, close_ducklake

    if year is None:
        try:
            from airflow.sdk import get_current_context
            ctx = get_current_context()
            year = int(ctx.get("params", {}).get("year", 2023))
        except Exception:
            year = 2023

    def cols_info(con, table: str) -> dict[str, str]:
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        return {r[1]: r[2] for r in rows}

    def pick(cols: dict[str, str], candidates: list[str], label: str) -> str:
        for c in candidates:
            if c in cols:
                return c
        raise ValueError(
            f"[gold_long_trip_dependency] Missing column for {label}. "
            f"Tried: {candidates}. Available: {sorted(cols.keys())}"
        )

    con = None
    try:
        con = connect_ducklake()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        # --- Trips (silver_mobility_trips)
        tcols = cols_info(con, "silver_mobility_trips")
        origin_col = pick(tcols, ["origin_zone_id", "origin_zone"], "origin")
        dest_col   = pick(tcols, ["destination_zone_id", "destination_zone"], "destination")
        trips_col  = pick(tcols, ["total_trips", "trips"], "trip count")

        # --- Geometry (gold_geometry_wgs84)
        gcols = cols_info(con, "gold_geometry_wgs84")
        muni_col = pick(gcols, ["municipality_id", "zone_id"], "municipality_id")

        geom_col = "geometry" if "geometry" in gcols else None
        cent_col = "centroid" if "centroid" in gcols else None
        year_col = "year" if "year" in gcols else None

        if geom_col is None and cent_col is None:
            raise ValueError("[gold_long_trip_dependency] gold_geometry_wgs84 needs 'geometry' or 'centroid'.")

        geo_year_filter = f"WHERE {year_col} = {year}" if year_col else ""

        # Centroids per municipality (study polygon)
        if geom_col:
            muni_centroids_cte = f"""
            muni_centroids AS (
              SELECT
                CAST({muni_col} AS VARCHAR) AS municipality_id,
                ST_Centroid(ST_Union_Agg({geom_col})) AS centroid
              FROM gold_geometry_wgs84
              {geo_year_filter}
              GROUP BY {muni_col}
            )
            """
        else:
            muni_centroids_cte = f"""
            muni_centroids AS (
              SELECT
                CAST({muni_col} AS VARCHAR) AS municipality_id,
                ST_Point(AVG(ST_X({cent_col})), AVG(ST_Y({cent_col}))) AS centroid
              FROM gold_geometry_wgs84
              {geo_year_filter}
              GROUP BY {muni_col}
            )
            """

        # --- Gold table: correct dependency (origin in polygon, destination anywhere)
        con.execute(f"""
        CREATE OR REPLACE TABLE gold_long_trip_dependency AS
        WITH
        {muni_centroids_cte},

        trips AS (
          SELECT
            -- Normalize IDs: remove _AM/_AD and keep only digits
            regexp_replace(replace(replace(CAST({origin_col} AS VARCHAR), '_AM', ''), '_AD', ''), '[^0-9]', '', 'g') AS origin_zone_id,
            regexp_replace(replace(replace(CAST({dest_col}   AS VARCHAR), '_AM', ''), '_AD', ''), '[^0-9]', '', 'g') AS destination_zone_id,
            TRY_CAST({trips_col} AS DOUBLE) AS total_trips
          FROM silver_mobility_trips
          WHERE {trips_col} IS NOT NULL
        ),

        -- ✅ Keep only origins inside the polygon
        origin_filtered AS (
          SELECT t.*
          FROM trips t
          JOIN muni_centroids o
            ON t.origin_zone_id = o.municipality_id
          WHERE t.origin_zone_id IS NOT NULL
            AND t.destination_zone_id IS NOT NULL
            AND t.total_trips IS NOT NULL
        ),

        -- Compute distance when destination centroid exists (destination may be outside polygon -> distance NULL)
        with_dist AS (
          SELECT
            f.origin_zone_id,
            f.destination_zone_id,
            f.total_trips,
            {year} AS year,
            CASE
              WHEN d.centroid IS NULL THEN NULL
              ELSE ST_Distance_Spheroid(o.centroid, d.centroid) / 1000.0
            END AS distance_km
          FROM origin_filtered f
          JOIN muni_centroids o
            ON f.origin_zone_id = o.municipality_id
          LEFT JOIN muni_centroids d
            ON f.destination_zone_id = d.municipality_id
        ),

        agg AS (
          SELECT
            year,
            origin_zone_id,
            SUM(total_trips) AS total_trips,

            -- Long trips only when distance is known
            SUM(CASE WHEN distance_km IS NOT NULL AND distance_km > 15 THEN total_trips ELSE 0 END) AS long_trips,

            -- Average distance only over known distances
            AVG(distance_km) AS avg_trip_km,

            -- Useful QA metric: share of trips with known distance
            SUM(CASE WHEN distance_km IS NOT NULL THEN total_trips ELSE 0 END) AS trips_with_distance
          FROM with_dist
          GROUP BY year, origin_zone_id
        )

        SELECT
          year,
          origin_zone_id AS municipality_id,
          total_trips,
          long_trips,
          trips_with_distance,
          CASE
            WHEN total_trips = 0 THEN NULL
            ELSE long_trips * 1.0 / total_trips
          END AS long_trip_ratio,
          avg_trip_km
        FROM agg;
        """)

        # --- Map (Kepler.gl)
        if generate_map:
            from keplergl import KeplerGl

            output_dir = "/usr/local/airflow/include/outputs"
            os.makedirs(output_dir, exist_ok=True)
            output_path = f"{output_dir}/long_trip_dependency_map_{year}.html"

            # Join geometry for all municipalities in polygon (some may have no trips -> they won't appear if we require gold rows)
            df = con.execute(f"""
                SELECT
                    d.municipality_id,
                    d.year,
                    d.total_trips,
                    d.long_trips,
                    d.trips_with_distance,
                    d.long_trip_ratio,
                    COALESCE(d.avg_trip_km, 0) AS avg_trip_km,
                    CASE
                        WHEN d.long_trip_ratio IS NULL THEN 0
                        WHEN d.long_trip_ratio < 0.20 THEN 1
                        WHEN d.long_trip_ratio < 0.40 THEN 2
                        ELSE 3
                    END AS dependency_code,
                    ST_AsGeoJSON(ST_Union_Agg(g.geometry)) AS geometry
                FROM gold_long_trip_dependency d
                JOIN gold_geometry_wgs84 g
                    ON CAST(d.municipality_id AS VARCHAR) = CAST(g.municipality_id AS VARCHAR)
                WHERE d.year = {year}
                GROUP BY
                    d.municipality_id,
                    d.year,
                    d.total_trips,
                    d.long_trips,
                    d.trips_with_distance,
                    d.long_trip_ratio,
                    d.avg_trip_km
            """).fetchdf()

            print(f"✓ Municipalities plotted (gold rows): {len(df)}")

            map_kepler = KeplerGl(height=700)
            map_kepler.add_data(data=df, name="long_trip_dependency")
            map_kepler.save_to_html(file_name=output_path)

            print(f"✓ Map saved to: {output_path}")

    finally:
        if con:
            close_ducklake(con)




