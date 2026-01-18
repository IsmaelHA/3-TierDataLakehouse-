from ducklake_utils import SILVER_MITMA_TABLE, GOLD_MITMA_TABLE

def create_gold_mitma_table(con):
    """
    Creates the structure for the Gold Patterns table.
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_MITMA_TABLE} (
            day_type INTEGER,
            hour_period INTEGER,
            origin_zone VARCHAR,
            destination_zone VARCHAR,
            total_trips DOUBLE,
            avg_trips DOUBLE,
            std_trips DOUBLE,
            num_days_observed INTEGER
        );
    """)
    print(f"✅ Table {GOLD_MITMA_TABLE} checked/created.")


def transform_gold_mitma(con):
    """
    Memory-optimized version: Computes patterns in a single pass without intermediate tables.
    """
    print("🔄 Starting Gold Aggregation (Memory-Optimized)...")
    
    try:
        # Strategy 1: Single-pass aggregation (no intermediate table)
        # This calculates everything in one query, reducing memory footprint
        con.execute(f"""
            CREATE OR REPLACE TABLE {GOLD_MITMA_TABLE} AS
            WITH stats AS (
                SELECT 
                    day_type,
                    hour_period,
                    origin_zone,
                    destination_zone,
                    SUM(trips) as total_trips,
                    AVG(trips) as avg_trips,
                    STDDEV_SAMP(trips) as std_trips,
                    COUNT(DISTINCT date) as num_days_observed
                FROM {SILVER_MITMA_TABLE}
                GROUP BY day_type, hour_period, origin_zone, destination_zone
            ),
            outlier_filtered AS (
                SELECT 
                    s.day_type,
                    s.hour_period,
                    s.origin_zone,
                    s.destination_zone,
                    s.trips,
                    s.date,
                    st.avg_trips,
                    st.std_trips
                FROM {SILVER_MITMA_TABLE} s
                JOIN stats st 
                    ON s.day_type = st.day_type 
                    AND s.hour_period = st.hour_period 
                    AND s.origin_zone = st.origin_zone 
                    AND s.destination_zone = st.destination_zone
                WHERE 
                    st.std_trips IS NULL 
                    OR st.std_trips = 0
                    OR (s.trips BETWEEN (st.avg_trips - 3 * st.std_trips) 
                                    AND (st.avg_trips + 3 * st.std_trips))
            )
            SELECT 
                day_type,
                hour_period,
                origin_zone,
                destination_zone,
                SUM(trips) as total_trips,
                AVG(trips) as avg_trips,
                COALESCE(STDDEV_SAMP(trips), 0) as std_trips,
                COUNT(DISTINCT date) as num_days_observed
            FROM outlier_filtered
            GROUP BY day_type, hour_period, origin_zone, destination_zone;
        """)
        
        count = con.execute(f"SELECT COUNT(*) FROM {GOLD_MITMA_TABLE}").fetchone()[0]
        print(f"✅ Gold Patterns updated. Total patterns: {count}")
        
    except Exception as e:
        print(f"❌ Transform to Gold Failed: {e}")
        raise e


def transform_gold_mitma_chunked(con, chunk_by='day_type'):
    """
    Alternative Strategy 2: Process data in chunks to reduce memory usage.
    Useful for extremely large datasets.
    """
    print(f"🔄 Starting Gold Aggregation (Chunked by {chunk_by})...")
    
    try:
        # Get unique chunk values
        chunks = con.execute(f"SELECT DISTINCT {chunk_by} FROM {SILVER_MITMA_TABLE}").fetchall()
        
        # Create/replace the table with first chunk
        first_chunk = True
        
        for (chunk_val,) in chunks:
            print(f"  Processing {chunk_by} = {chunk_val}...")
            
            query = f"""
                {'CREATE OR REPLACE' if first_chunk else 'INSERT INTO'} TABLE {GOLD_MITMA_TABLE}
                WITH stats AS (
                    SELECT 
                        day_type, hour_period, origin_zone, destination_zone,
                        AVG(trips) as avg_trips,
                        STDDEV_SAMP(trips) as std_trips
                    FROM {SILVER_MITMA_TABLE}
                    WHERE {chunk_by} = {chunk_val}
                    GROUP BY day_type, hour_period, origin_zone, destination_zone
                ),
                filtered AS (
                    SELECT s.*
                    FROM {SILVER_MITMA_TABLE} s
                    JOIN stats st 
                        ON s.day_type = st.day_type 
                        AND s.hour_period = st.hour_period 
                        AND s.origin_zone = st.origin_zone 
                        AND s.destination_zone = st.destination_zone
                    WHERE s.{chunk_by} = {chunk_val}
                        AND (st.std_trips IS NULL OR st.std_trips = 0
                             OR (s.trips BETWEEN (st.avg_trips - 3 * st.std_trips) 
                                             AND (st.avg_trips + 3 * st.std_trips)))
                )
                SELECT 
                    day_type, hour_period, origin_zone, destination_zone,
                    SUM(trips) as total_trips,
                    AVG(trips) as avg_trips,
                    COALESCE(STDDEV_SAMP(trips), 0) as std_trips,
                    COUNT(DISTINCT date) as num_days_observed
                FROM filtered
                GROUP BY day_type, hour_period, origin_zone, destination_zone;
            """
            
            con.execute(query)
            first_chunk = False
        
        count = con.execute(f"SELECT COUNT(*) FROM {GOLD_MITMA_TABLE}").fetchone()[0]
        print(f"✅ Gold Patterns updated. Total patterns: {count}")
        
    except Exception as e:
        print(f"❌ Chunked transform failed: {e}")
        raise e


def transform_gold_mitma_streaming(con):
    """
    Alternative Strategy 3: Skip outlier filtering if it's the bottleneck.
    Calculate stats directly without the join-back step.
    """
    print("🔄 Starting Gold Aggregation (Direct Stats, No Outlier Filter)...")
    
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {GOLD_MITMA_TABLE} AS
            SELECT 
                day_type,
                hour_period,
                origin_zone,
                destination_zone,
                SUM(trips) as total_trips,
                AVG(trips) as avg_trips,
                COALESCE(STDDEV_SAMP(trips), 0) as std_trips,
                COUNT(DISTINCT date) as num_days_observed
            FROM {SILVER_MITMA_TABLE}
            GROUP BY day_type, hour_period, origin_zone, destination_zone;
        """)
        
        count = con.execute(f"SELECT COUNT(*) FROM {GOLD_MITMA_TABLE}").fetchone()[0]
        print(f"✅ Gold Patterns updated (no outlier filter). Total patterns: {count}")
        
    except Exception as e:
        print(f"❌ Direct transform failed: {e}")
        raise e