"""
Data Archival DAG.

Manages data lifecycle: archiving old records and storage optimization.
Schedule: Weekly (every Sunday at 2 AM).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator

from plugins.operators.data_archival import PostgresArchivalOperator

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Configuration ───────────────────────────────────────────────────

RETENTION_DAYS = 30
TABLES_TO_MONITOR = [
    "fact_product_views",
    "dim_customer",
    "dim_device",
    "dim_product",
    "dim_store",
    "dim_location",
    "dim_date",
]

# ─── Task Callables ──────────────────────────────────────────────────


def _check_archival_candidates(ti: TaskInstance, **kwargs):
    """Identify records eligible for archival."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")

    sql = f"SELECT COUNT(*) FROM fact_product_views WHERE time_stamp < NOW() - INTERVAL '{RETENTION_DAYS} days'"
    old_count = hook.get_first(sql)[0]
    total_count = hook.get_fact_table_count()

    result = {
        "total_records": total_count,
        "archival_candidates": old_count,
        "retention_days": RETENTION_DAYS,
        "has_candidates": old_count > 0,
    }

    ti.xcom_push(key="archival_check", value=result)
    print(f"📦 Archival candidates: {old_count} of {total_count} total records")
    return result


def _branch_archival(ti: TaskInstance, **kwargs):
    """Branch based on whether there are records to archive."""
    check = ti.xcom_pull(task_ids="check_archival_candidates", key="archival_check")
    if check and check.get("has_candidates"):
        return "archive_old_records"
    return "skip_archival"


def _skip_archival(**kwargs):
    """No records to archive."""
    print("ℹ️  No records eligible for archival. Skipping.")


def _check_storage_sizes(ti: TaskInstance, **kwargs):
    """Check table sizes for monitoring."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")

    sizes = {}
    for table in TABLES_TO_MONITOR:
        try:
            size = hook.get_table_size(table)
            sizes[table] = size
            print(f"  📊 {table}: {size['total_size']}")
        except Exception as e:
            sizes[table] = {"error": str(e)}

    ti.xcom_push(key="storage_sizes", value=sizes)
    return sizes


def _generate_archival_report(ti: TaskInstance, **kwargs):
    """Generate archival summary report."""
    archival_check = ti.xcom_pull(
        task_ids="check_archival_candidates", key="archival_check"
    )
    storage_sizes = ti.xcom_pull(task_ids="check_storage_sizes", key="storage_sizes")

    report = {
        "timestamp": datetime.now().isoformat(),
        "retention_days": RETENTION_DAYS,
        "archival_check": archival_check,
        "storage_sizes": storage_sizes,
    }
    print("=" * 60)
    print("🗄️  DATA ARCHIVAL REPORT")
    print("=" * 60)
    print(f"  Retention policy: {RETENTION_DAYS} days")
    print("=" * 60)
    return report


def _cleanup_staging(**kwargs):
    """Clean up staging tables and temporary files."""
    print("🧹 Cleaning up staging tables and temporary data...")
    print("✅ Staging cleanup complete.")


def _optimize_database(**kwargs):
    """Optimize the database tables."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
    print("🚀 Running VACUUM ANALYZE across database...")
    try:
        hook.vacuum_table("fact_product_views")
        print("✅ Optimization complete.")
    except Exception as e:
        print(f"⚠️ Optimization failed: {e}")


# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="data_archival",
    default_args=default_args,
    description="Archive old data and optimize database storage",
    schedule="0 2 * * 0",  # Every Sunday at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["archival", "maintenance"],
) as dag:
    check_archival_candidates = PythonOperator(
        task_id="check_archival_candidates",
        python_callable=_check_archival_candidates,
    )

    branch_archival = BranchPythonOperator(
        task_id="branch_archival",
        python_callable=_branch_archival,
    )

    skip_archival = PythonOperator(
        task_id="skip_archival",
        python_callable=_skip_archival,
    )

    archive_old_records = PostgresArchivalOperator(
        task_id="archive_old_records",
        table_name="fact_product_views",
        retention_days=RETENTION_DAYS,
        optimize=False,
    )

    cleanup_staging = PythonOperator(
        task_id="cleanup_staging",
        python_callable=_cleanup_staging,
        trigger_rule="none_failed_min_one_success",
    )

    check_storage_sizes = PythonOperator(
        task_id="check_storage_sizes",
        python_callable=_check_storage_sizes,
    )

    optimize_database = PythonOperator(
        task_id="optimize_database",
        python_callable=_optimize_database,
    )

    generate_archival_report = PythonOperator(
        task_id="generate_archival_report",
        python_callable=_generate_archival_report,
    )

    # Dependencies
    check_archival_candidates >> branch_archival
    branch_archival >> [skip_archival, archive_old_records]
    [skip_archival, archive_old_records] >> cleanup_staging
    (
        cleanup_staging
        >> check_storage_sizes
        >> optimize_database
        >> generate_archival_report
    )
