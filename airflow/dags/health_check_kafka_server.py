"""
Kafka Remote Health Check DAG.

Performs a network-level connectivity check (socket ping) to the
Remote Kafka source cluster to ensure the data source is available.

Schedule: Every 5 minutes.
"""

import re
import socket
from contextlib import closing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance, Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator

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


def _health_check(ti: TaskInstance, **kwargs):
    """Pings the remote Kafka servers and returns health status."""
    remote_servers = Variable.get("SERVER_BOOTSTRAP_SERVERS", default_var=None)
    if not remote_servers:
        print("No SERVER_BOOTSTRAP_SERVERS configured.")
        ti.xcom_push(key="is_healthy", value=False)
        return False

    # Clean up accidental "Val (Value): " prefix from Airflow UI
    remote_servers = re.sub(
        r"^Val\s*\(Value\):\s*", "", remote_servers, flags=re.IGNORECASE
    ).strip()

    is_healthy = False
    for server in remote_servers.split(","):
        try:
            server = server.strip()
            if not server:
                continue
            host_port = server.split(":")
            host = host_port[0].strip()
            port = int(host_port[1].strip()) if len(host_port) > 1 else 9092

            print(f"Checking {host}:{port}...")
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(10)
                if sock.connect_ex((host, port)) == 0:
                    print(f"✅ Reachable: {host}:{port}")
                    is_healthy = True
                    break
        except Exception as e:
            print(f"Failed to check {server}: {e}")
            continue

    if not is_healthy:
        print("❌ None of the remote brokers are reachable.")

    ti.xcom_push(key="is_healthy", value=is_healthy)
    return is_healthy


def _branch(ti: TaskInstance, **kwargs):
    """Branch based on the health check result."""
    is_healthy = ti.xcom_pull(task_ids="health_check", key="is_healthy")
    return "server_is_up" if is_healthy else "server_is_down"


def _server_up_action():
    print("🚀 Remote Kafka cluster is UP and reachable. Pipeline is healthy.")


def _server_down_action():
    print("⚠️  Remote Kafka cluster is DOWN! Needs immediate attention.")


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
    health_check = PythonOperator(
        task_id="health_check",
        python_callable=_health_check,
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=_branch,
    )

    server_is_up = PythonOperator(
        task_id="server_is_up",
        python_callable=_server_up_action,
    )

    server_is_down = PythonOperator(
        task_id="server_is_down",
        python_callable=_server_down_action,
    )

    # Branching logic
    health_check >> branch >> [server_is_up, server_is_down]
