"""
Kafka Producer Control DAG.

Manages the lifecycle of the Kafka producer script that forwards data
from the remote server to the local Kafka cluster.

Configuration is loaded from Airflow Variables and Connections – no
credentials are hardcoded in this file.

Required Airflow setup (UI → Admin):
  Variables:
    - project_path        : absolute path to the project on the host
                            (default: /app)
  Connections (conn_id = "local_kafka_conn"):
    - Conn Type : Generic (or HTTP)
    - Host      : kafka-0:9092,kafka-1:9092,kafka-2:9092
    - Login     : <sasl username>
    - Password  : <sasl password>
    - Extra     : {"topic": "product_view",
                   "security_protocol": "SASL_PLAINTEXT",
                   "sasl_mechanism": "PLAIN"}
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ─── Configuration (resolved at parse time from Airflow store) ────────

PROJECT_PATH = Variable.get("project_path", default_var="/app")

_kafka_conn = BaseHook.get_connection("local_kafka_conn")
_kafka_extra = json.loads(_kafka_conn.extra or "{}")

LOCAL_BOOTSTRAP_SERVERS = _kafka_conn.host or "kafka-0:9092,kafka-1:9092,kafka-2:9092"
LOCAL_SASL_USERNAME = _kafka_conn.login
LOCAL_SASL_PASSWORD = _kafka_conn.password
LOCAL_TOPIC = _kafka_extra.get("topic", "product_view")
LOCAL_SECURITY_PROTOCOL = _kafka_extra.get("security_protocol", "SASL_PLAINTEXT")
LOCAL_SASL_MECHANISM = _kafka_extra.get("sasl_mechanism", "PLAIN")

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="kafka_producer_control",
    default_args=default_args,
    description="Manage Kafka producer (Remote -> Local forwarder)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kafka", "producer", "ingestion"],
) as dag:

    run_producer = DockerOperator(
        task_id="run_kafka_producer",
        image="python:3.11-slim",
        auto_remove="force",
        mount_tmp_dir=False,
        command=[
            "bash", "-c",
            "apt-get update && apt-get install -y ca-certificates && "
            "pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org "
            "confluent-kafka python-dotenv && "
            "export PYTHONPATH=$PYTHONPATH:/app && "
            "python -u /app/src/kafka/producer.py"
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="streaming-network",
        working_dir="/app",
        mounts=[
            Mount(source=PROJECT_PATH, target="/app", type="bind"),
        ],
        environment={
            "LOCAL_BOOTSTRAP_SERVERS": LOCAL_BOOTSTRAP_SERVERS,
            "LOCAL_TOPIC": LOCAL_TOPIC,
            "LOCAL_SECURITY_PROTOCOL": LOCAL_SECURITY_PROTOCOL,
            "LOCAL_SASL_MECHANISM": LOCAL_SASL_MECHANISM,
            "LOCAL_SASL_USERNAME": LOCAL_SASL_USERNAME,
            "LOCAL_SASL_PASSWORD": LOCAL_SASL_PASSWORD,
            "PYTHONUNBUFFERED": "1",
        },
    )

    run_producer
