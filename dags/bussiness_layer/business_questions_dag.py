""" 
Unified DAG: Business Questions Analysis

Combines three business questions:
1. Typical Day Patterns (generate_mobility_report_local)
2. Gravity Model (gravity)
3. Long Trip Dependency

Outputs:
- PDF report + CSV in include/outputs
- infrastructure_map.html (Kepler)
- gold_long_trip_dependency table

Notes:
- DAG params include start_date and end_date (YYYY-MM-DD) but tasks do not use them yet.
- The parameter year is not exposed; everything is forced to 2023 internally.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Gravity imports
from gravity.extract_geometry import extract_geometry, DEFAULT_WKT
from gravity.verify_dependencies import verify_dependencies
from gravity.create_centroids import create_municipality_centroids
from gravity.create_distances import create_municipality_distances
from gravity.aggregate_trips import aggregate_trips
from gravity.aggregate_economy import aggregate_economy
from gravity.create_gravity_data import create_gravity_data
from gravity.calculate_gold import calculate_and_create_gold
from gravity.create_ranking import create_infrastructure_ranking
from gravity.create_map import create_infrastructure_map
from gravity.cleanup import cleanup_temp_tables

# Long trip dependency
from bussiness_layer.transform_gold_long_trip_dependency import transform_gold_long_trip_dependency

# Report generation
from bussiness_layer.generate_report import generate_mobility_report_local

from ducklake_utils import connect_ducklake, close_ducklake


# =============================================================================
# CONFIG
# =============================================================================
FORCED_YEAR = 2023
REPORT_OUTPUT_DIR = "include/outputs"


def _supports_var_kwargs(func) -> bool:
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())


def _inject_year_kwargs(func, year: int) -> dict:
    """Inject year/target_year only if the callable supports it (or **kwargs)."""
    if _supports_var_kwargs(func):
        return {"year": year}

    try:
        params = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return {}

    if "year" in params:
        return {"year": year}
    if "target_year" in params:
        return {"target_year": year}
    return {}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="business_questions_pipeline",
    default_args=default_args,
    description="Unified pipeline: Typical Patterns + Gravity Model + Long Trip Dependency",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=1,
    tags=["business", "gold", "analysis", "gravity", "report"],
    params={
        "wkt_polygon": Param(
            default=DEFAULT_WKT,
            type="string",
            description="WKT polygon in EPSG:4326 (WGS84)",
        ),
        "spatial_predicate": Param(
            default="intersects",
            type="string",
            enum=["intersects", "contains", "within"],
            description="Spatial predicate",
        ),
        "start_date": Param(
            default=f"{FORCED_YEAR}-01-01",
            type="string",
            description="Start date (YYYY-MM-DD). Placeholder only (not used yet).",
        ),
        "end_date": Param(
            default=f"{FORCED_YEAR}-12-31",
            type="string",
            description="End date (YYYY-MM-DD). Placeholder only (not used yet).",
        ),
    },
) as dag:

    # =========================================================================
    # TASK GROUP 1: TYPICAL DAY PATTERNS (Business Question 1)
    # =========================================================================
    with TaskGroup("bq1_typical_patterns", tooltip="Business Question 1: Typical Day Patterns") as bq1_group:

        def _generate_report(**context):
            con = None
            try:
                con = connect_ducklake()

                # Auto-select ALL district_id values present in gold_geometry_wgs84.
                rows = con.execute(
                    """
                    SELECT DISTINCT district_id
                    FROM gold_geometry_wgs84
                    WHERE district_id IS NOT NULL
                    ORDER BY district_id
                    """
                ).fetchall()
                target_districts = [r[0] for r in rows]

                if not target_districts:
                    print("No district_id found in gold_geometry_wgs84. Report skipped.")
                    return

                kwargs = {
                    "con": con,
                    # NOTE: generate_mobility_report_local interprets target_origins as district_id values.
                    "target_origins": target_districts,
                    "output_dir": REPORT_OUTPUT_DIR,
                    "pdf_filename": "day_profiles.pdf",
                }

                # Force year to 2023, but only if supported by the callable.
                kwargs.update(_inject_year_kwargs(generate_mobility_report_local, FORCED_YEAR))

                generate_mobility_report_local(**kwargs)
            finally:
                if con:
                    close_ducklake(con)

        t_report = PythonOperator(task_id="generate_mobility_report", python_callable=_generate_report)

    # =========================================================================
    # TASK GROUP 2: GRAVITY MODEL (Business Question 2)
    # =========================================================================
    with TaskGroup("bq2_gravity_model", tooltip="Business Question 2: Gravity Model") as bq2_group:

        def _extract_geometry(**context):
            wkt = context["params"].get("wkt_polygon", DEFAULT_WKT)
            predicate = context["params"].get("spatial_predicate", "intersects")
            extract_geometry(wkt, predicate)

        def _aggregate_trips(**context):
            kwargs = _inject_year_kwargs(aggregate_trips, FORCED_YEAR)
            if kwargs:
                aggregate_trips(**kwargs)
            else:
                aggregate_trips()

        def _aggregate_economy(**context):
            aggregate_economy(FORCED_YEAR)

        def _create_gravity_data(**context):
            create_gravity_data(FORCED_YEAR)

        t_extract = PythonOperator(task_id="extract_geometry", python_callable=_extract_geometry)
        t_verify = PythonOperator(task_id="verify_dependencies", python_callable=verify_dependencies)
        t_centroids = PythonOperator(task_id="create_centroids", python_callable=create_municipality_centroids)
        t_distances = PythonOperator(task_id="create_distances", python_callable=create_municipality_distances)
        t_trips = PythonOperator(task_id="aggregate_trips", python_callable=_aggregate_trips)
        t_economy = PythonOperator(task_id="aggregate_economy", python_callable=_aggregate_economy)
        t_gravity = PythonOperator(task_id="create_gravity_data", python_callable=_create_gravity_data)
        t_gold = PythonOperator(task_id="create_gold", python_callable=calculate_and_create_gold)
        t_ranking = PythonOperator(task_id="create_ranking", python_callable=create_infrastructure_ranking)
        t_map = PythonOperator(task_id="create_map", python_callable=create_infrastructure_map)
        t_cleanup = PythonOperator(task_id="cleanup", python_callable=cleanup_temp_tables)

        t_extract >> t_verify >> t_centroids >> t_distances >> [t_trips, t_economy] >> t_gravity >> t_gold >> t_ranking >> t_map >> t_cleanup

    # =========================================================================
    # TASK GROUP 3: LONG TRIP DEPENDENCY (Business Question 3)
    # =========================================================================
    with TaskGroup("bq3_long_trip_dependency", tooltip="Business Question 3: Long Trip Dependency") as bq3_group:

        def _long_trip_dependency(**context):
            kwargs = _inject_year_kwargs(transform_gold_long_trip_dependency, FORCED_YEAR)
            if kwargs:
                transform_gold_long_trip_dependency(**kwargs)
            else:
                transform_gold_long_trip_dependency()

        t_long_trip = PythonOperator(
            task_id="create_long_trip_dependency",
            python_callable=_long_trip_dependency,
        )

    # =========================================================================
    # MAIN FLOW: run BQ2 first, then run BQ1 and BQ3
    # =========================================================================
    bq2_group >> [bq1_group, bq3_group]
