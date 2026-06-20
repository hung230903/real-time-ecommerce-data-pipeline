"""
Data Quality DAG.

Runs comprehensive quality checks on the star schema:
completeness, data types, business rules, error rates,
and generates quality reports.

Schedule: Every 15 minutes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator

from plugins.operators.data_quality import DataQualityOperator

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ─── Quality Check Tables ─────────────────────────────────────────────

STAR_SCHEMA_TABLES = [
    "fact_product_views",
    "dim_customer",
    "dim_device",
    "dim_product",
    "dim_store",
    "dim_location",
    "dim_date",
]

# ─── Task Callables ──────────────────────────────────────────────────


def _check_completeness(ti: TaskInstance, **kwargs):
    """Check data completeness across all star schema tables."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
    results = {}

    for table in STAR_SCHEMA_TABLES:
        completeness = hook.check_table_completeness(table)
        results[table] = completeness
        pct = completeness["completeness_pct"]
        emoji = "✅" if pct >= 90 else "⚠️" if pct >= 70 else "❌"
        print(f"  {emoji} {table}: {pct}% complete ({completeness['total_rows']} rows)")

    ti.xcom_push(key="completeness_results", value=results)
    return results


def _check_data_types(ti: TaskInstance, **kwargs):
    """Validate data types across star schema tables."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
    results = {}

    for table in STAR_SCHEMA_TABLES:
        dtypes = hook.validate_data_types(table)
        results[table] = dtypes
        print(f"  📋 {table}: {len(dtypes)} columns validated")

    ti.xcom_push(key="data_type_results", value=results)
    return results


def _monitor_error_rates(ti: TaskInstance, **kwargs):
    """Monitor error rates across tables."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
    results = {}

    for table in STAR_SCHEMA_TABLES:
        error_info = hook.get_error_rate(table, expected_min=0)
        results[table] = error_info
        emoji = "✅" if error_info["meets_minimum"] else "❌"
        print(f"  {emoji} {table}: {error_info['total_rows']} rows")

    ti.xcom_push(key="error_rate_results", value=results)
    return results


def _generate_quality_report(ti: TaskInstance, **kwargs):
    """Summarize data quality results."""
    completeness = ti.xcom_pull(
        task_ids="check_completeness", key="completeness_results"
    )
    data_types = ti.xcom_pull(task_ids="check_data_types", key="data_type_results")
    error_rates = ti.xcom_pull(task_ids="monitor_error_rates", key="error_rate_results")

    report = {
        "timestamp": datetime.now().isoformat(),
        "completeness": completeness,
        "data_types": data_types,
        "error_rates": error_rates,
    }

    print("=" * 60)
    print("📊 DATA QUALITY REPORT")
    print("=" * 60)
    print(f"  Timestamp: {report['timestamp']}")
    print(f"  Tables checked: {len(STAR_SCHEMA_TABLES)}")
    print("=" * 60)

    return report


# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Run comprehensive data quality checks on star schema",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["quality", "monitoring"],
) as dag:
    completeness = PythonOperator(
        task_id="check_completeness",
        python_callable=_check_completeness,
    )

    data_types = PythonOperator(
        task_id="check_data_types",
        python_callable=_check_data_types,
    )

    # SUCCESS: Using the custom operator for business rules
    business_rules = DataQualityOperator(
        task_id="check_business_rules",
        postgres_conn_id="postgres_streaming",
        sql_checks=[
            {
                "name": "no_orphan_customer",
                "sql": "SELECT COUNT(*) FROM fact_product_views f LEFT JOIN dim_customer d ON f.customer_id = d.customer_id WHERE d.customer_id IS NULL",
                "expected_result": 0,
            },
            {
                "name": "no_orphan_product",
                "sql": "SELECT COUNT(*) FROM fact_product_views f LEFT JOIN dim_product d ON f.product_id = d.product_id WHERE d.product_id IS NULL",
                "expected_result": 0,
            },
            {
                "name": "no_duplicate_events",
                "sql": "SELECT COUNT(*) - COUNT(DISTINCT fact_id) FROM fact_product_views",
                "expected_result": 0,
            },
        ],
    )

    error_rates = PythonOperator(
        task_id="monitor_error_rates",
        python_callable=_monitor_error_rates,
    )

    quality_report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=_generate_quality_report,
    )

    # Quality checks run in parallel, then generate report
    [completeness, data_types, business_rules, error_rates] >> quality_report
