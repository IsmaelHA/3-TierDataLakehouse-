from graphviz import Digraph

def create_schema_plot(filename='schema_plot'):
    dot = Digraph(comment='MITMA Schema', format='png')
    dot.attr(rankdir='LR')
    dot.attr('node', shape='record', style='filled', fillcolor='#f0f0f0')

    # Define the tables and columns dictionary based on your logs
    tables = {
        "bronze_calendar_dates": ["date (DATE)", "day_of_week (INT)", "is_holiday (BOOL)"],
        "silver_stats_log": ["processed_date (DATE)"],
        "silver_zone_stats": ["day_type (INT)", "hour_period (INT)", "origin_zone (VARCHAR)", "destination_zone (VARCHAR)", "mean_trips (DOUBLE)"],
        "stg_mobility_clean_check": ["date (DATE)", "hour_period (INT)", "origin_zone (VARCHAR)", "destination_zone (VARCHAR)", "trips (DOUBLE)"],
        "silver_population": ["municipality_code (VARCHAR)", "year (INT)", "population (BIGINT)"],
        "bronze_economy_2022": ["Municipios", "Distritos", "Secciones", "Renta_Media", "Total"],
        "silver_economy_aggregated": ["municipality_code", "district_code", "section_code", "year (INT)", "avg_income (DOUBLE)"],
        "silver_geometry_wgs84": ["geometry (GEOM)", "census_section_id", "district_id", "municipality_id", "year (INT)"],
        "bronze_geometry_2023": ["geom (GEOM)", "CUSEC", "CUDIS", "CUMUN", "CPRO", "CCA"],
        "ref_holidays": ["date (DATE)", "is_holiday (BOOL)"]
    }

    # Add Nodes
    for table, columns in tables.items():
        # Create label string for Graphviz
        # Format: { TableName | col1 \l col2 \l ... }
        col_str = " \\l ".join(columns)
        label = f"{{ {table} | {col_str} \\l }}"
        dot.node(table, label=label)

    # Note: Your provided SQL has no FOREIGN KEYS defined.
    # If you want to draw lines between tables, you must add edges manually like this:
    # dot.edge('silver_zone_stats', 'silver_geometry_wgs84', label='zones')

    output_path = dot.render(filename, cleanup=True)
    print(f"Plot saved to {output_path}")

create_schema_plot()