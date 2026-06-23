"""
Kafka Remote Health Check DAG.

Performs a network-level connectivity check (socket ping) to the
Remote Kafka source cluster to ensure the data source is available.

Schedule: Every 5 minutes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from plugins.sensors.kafka_sensor import KafkaBrokerSensor

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Task Callables ──────────────────────────────────────────────────


def _server_up_action():
    print("🚀 Remote Kafka cluster is UP and reachable. Pipeline is healthy.")


def _server_down_action():
    print("⚠️  Remote Kafka cluster is DOWN! Needs immediate attention.")
    # Raise Exception để Airflow đánh dấu task này (và DAG) là FAILED (Màu đỏ)
    raise ValueError("Kafka cluster is completely unreachable!")


# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="kafka_remote_health_check",
    default_args=default_args,
    description="Check connectivity to remote Kafka brokers with branching",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["monitoring", "kafka", "remote"],
) as dag:
    health_check = KafkaBrokerSensor(
        task_id="health_check",
        kafka_conn_id="kafka_default",
        bootstrap_servers=Variable.get("SERVER_BOOTSTRAP_SERVERS", default_var=""),
        conn_timeout=5,
        poke_interval=30,
        timeout=60,  # Fail if down for more than 1 minute
        mode="poke",
    )

    server_is_up = PythonOperator(
        task_id="server_is_up",
        python_callable=_server_up_action,
    )

    server_is_down = PythonOperator(
        task_id="server_is_down",
        python_callable=_server_down_action,
        # If health_check failed -> run this branch
        trigger_rule="one_failed",  # Chỉ chạy khi health_check bị FAILED/Timeout
    )

    health_check >> [server_is_up, server_is_down]
