"""
Spark Job Control DAG.

Manages the lifecycle of the Spark Structured Streaming job:
submission, monitoring, and cleanup.

Configuration is loaded from Airflow Variables and Connections – no
credentials are hardcoded in this file.

Required Airflow setup (UI → Admin):
  Variables:
    - project_path        : absolute path to the project on the host
                            (default: /app)
    - kafka_target_topic  : Kafka topic to consume
                            (default: product_view)
  Connections:
    - conn_id "postgres_streaming":
        Conn Type : Postgres
        Host      : <postgres host / docker bridge IP>
        Port      : 5432
        Schema    : spark_streaming_schema
        Login     : <postgres user>
        Password  : <postgres password>
    - conn_id "kafka_default":
        Conn Type : Generic
        Host      : <kafka bootstrap servers>
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance, Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator

from plugins.operators.spark_job import SparkStreamingOperator

# ─── Configuration (resolved at parse time from Airflow store) ────────

PROJECT_PATH = Variable.get("project_path", default_var="/app")
KAFKA_TOPIC = Variable.get("kafka_target_topic", default_var="product_view")

_pg_conn = BaseHook.get_connection("postgres_streaming")
_kafka_conn = BaseHook.get_connection("kafka_default")

PG_HOST = _pg_conn.host
PG_PORT = str(_pg_conn.port or 5432)
PG_DB = _pg_conn.schema or "spark_streaming_schema"
PG_USER = _pg_conn.login
PG_PASSWORD = _pg_conn.password

KAFKA_BOOTSTRAP_SERVERS = _kafka_conn.host or "kafka-0:9092,kafka-1:9092,kafka-2:9092"

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── Task Callables ──────────────────────────────────────────────────


def _pre_flight_checks(ti: TaskInstance, **kwargs):
    """Run connectivity checks before submitting Spark job."""
    checks = {"kafka": False, "postgres": False}

    try:
        from plugins.hooks.kafka_hook import KafkaMonitoringHook

        hook = KafkaMonitoringHook(kafka_conn_id="kafka_default")
        checks["kafka"] = hook.check_broker_connectivity(timeout=5)
    except:
        checks["kafka"] = False

    try:
        from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

        pg_hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
        pg_hook.get_first("SELECT 1")
        checks["postgres"] = True
    except:
        checks["postgres"] = False

    ti.xcom_push(key="preflight_pass", value=all(checks.values()))
    print(f"🛫 Pre-flight checks: {checks}")
    return checks


def _branch_on_preflight(ti: TaskInstance, **kwargs):
    """Branch based on pre-flight results."""
    if ti.xcom_pull(task_ids="pre_flight_checks", key="preflight_pass"):
        return "submit_spark_streaming_job"
    return "preflight_failed"


def _preflight_failed(**kwargs):
    """Handle pre-flight failure."""
    raise ValueError("Pre-flight checks failed. Spark job aborted.")


def _post_job_status(ti: TaskInstance, **kwargs):
    """Check post-job status."""
    from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

    hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
    count = hook.get_fact_table_count()
    print(f"📊 Fact records count: {count}")


def _cleanup_resources(**kwargs):
    """Clean up Docker resources."""
    try:
        import docker

        client = docker.from_env()
        for c in client.containers.list(all=True):
            if c.name.startswith("airflow-spark-"):
                c.remove(force=True)
    except:
        pass


# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="spark_job_control",
    default_args=default_args,
    description="Manage Spark Streaming job lifecycle",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "streaming"],
) as dag:
    pre_flight = PythonOperator(
        task_id="pre_flight_checks",
        python_callable=_pre_flight_checks,
    )

    branch_preflight = BranchPythonOperator(
        task_id="branch_on_preflight",
        python_callable=_branch_on_preflight,
    )

    preflight_failed = PythonOperator(
        task_id="preflight_failed",
        python_callable=_preflight_failed,
    )

    # SUCCESS: all config injected from Airflow Variables / Connections
    submit_spark_job = SparkStreamingOperator(
        task_id="submit_spark_streaming_job",
        kafka_topic=KAFKA_TOPIC,
        project_path=PROJECT_PATH,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        postgres_host=PG_HOST,
        postgres_port=PG_PORT,
        postgres_db=PG_DB,
        postgres_user=PG_USER,
        postgres_password=PG_PASSWORD,
    )

    post_status = PythonOperator(
        task_id="post_job_status",
        python_callable=_post_job_status,
        trigger_rule="all_done",
    )

    cleanup = PythonOperator(
        task_id="cleanup_resources",
        python_callable=_cleanup_resources,
        trigger_rule="all_done",
    )

    # Dependencies
    pre_flight >> branch_preflight
    branch_preflight >> [submit_spark_job, preflight_failed]
    submit_spark_job >> post_status >> cleanup
    preflight_failed >> cleanup
