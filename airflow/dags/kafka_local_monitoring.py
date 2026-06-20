"""
Kafka Local Monitoring DAG.

Monitors the local Kafka cluster health, data flow throughput,
and consumer group lag.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator

from plugins.hooks.kafka_hook import KafkaMonitoringHook

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

# ─── Task Callables ──────────────────────────────────────────────────


def _check_local_broker_health(ti: TaskInstance, **kwargs):
    """Check if the local Kafka brokers are reachable."""
    hook = KafkaMonitoringHook(kafka_conn_id="kafka_default")
    is_healthy = hook.check_broker_connectivity(timeout=5)
    ti.xcom_push(key="is_healthy", value=is_healthy)
    return is_healthy


def _branch_on_health(ti: TaskInstance, **kwargs):
    """Branch based on health check."""
    is_healthy = ti.xcom_pull(task_ids="check_local_broker_health", key="is_healthy")
    return "local_broker_healthy" if is_healthy else "local_broker_unhealthy"


def _local_broker_unhealthy():
    print("⚠️ Local Kafka Broker is UNHEALTHY!")


def _local_broker_healthy():
    print("✅ Local Kafka Broker is HEALTHY!")


def _check_local_data_flow(ti: TaskInstance, **kwargs):
    """Monitor local data flow throughput and consumer lag."""
    hook = KafkaMonitoringHook(kafka_conn_id="kafka_default")

    try:
        throughput = hook.get_message_throughput_estimate()
        lag = hook.get_consumer_lag()
    except Exception as e:
        print(f"Failed to fetch data flow stats: {e}")
        throughput, lag = 0, -1

    flow_report = {"throughput": throughput, "consumer_lag": lag}
    ti.xcom_push(key="data_flow_report", value=flow_report)
    return flow_report


def _generate_local_monitoring_report(ti: TaskInstance, **kwargs):
    """Generate combined local monitoring report."""
    is_healthy = ti.xcom_pull(task_ids="check_local_broker_health", key="is_healthy")
    data_flow = ti.xcom_pull(task_ids="check_local_data_flow", key="data_flow_report")

    report = {
        "timestamp": datetime.now().isoformat(),
        "broker_healthy": is_healthy,
        "data_flow": data_flow or {},
        "status": "active" if is_healthy else "down",
    }
    print(f"📊 Local Monitoring Report: {report}")
    return report


# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="kafka_local_monitoring",
    default_args=default_args,
    description="Monitor Local Kafka cluster health and data flow",
    schedule="*/2 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["monitoring", "kafka", "local"],
) as dag:
    check_local_broker_health = PythonOperator(
        task_id="check_local_broker_health",
        python_callable=_check_local_broker_health,
    )

    branch_on_health = BranchPythonOperator(
        task_id="branch_on_health",
        python_callable=_branch_on_health,
    )

    local_broker_unhealthy = PythonOperator(
        task_id="local_broker_unhealthy",
        python_callable=_local_broker_unhealthy,
    )

    local_broker_healthy = PythonOperator(
        task_id="local_broker_healthy",
        python_callable=_local_broker_healthy,
    )

    check_local_data_flow = PythonOperator(
        task_id="check_local_data_flow",
        python_callable=_check_local_data_flow,
    )

    generate_local_monitoring_report = PythonOperator(
        task_id="generate_local_monitoring_report",
        python_callable=_generate_local_monitoring_report,
        trigger_rule="none_failed_min_one_success",
    )

    # ─── Dependencies ────────────────────────────────────────────────

    check_local_broker_health >> [branch_on_health, check_local_data_flow]
    branch_on_health >> [local_broker_unhealthy, local_broker_healthy]
    [
        local_broker_unhealthy,
        local_broker_healthy,
        check_local_data_flow,
    ] >> generate_local_monitoring_report
