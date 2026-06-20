"""
Custom Kafka Hook for Airflow.

Provides reusable Kafka connectivity and monitoring methods
for use across multiple DAGs and operators.
"""

import socket
from contextlib import closing
from typing import Any, Dict

from airflow.hooks.base import BaseHook


class KafkaMonitoringHook(BaseHook):
    """
    Hook for monitoring Kafka cluster health and data flow.

    Uses a connection configured in Airflow with:
    - host: Kafka broker host
    - port: Kafka broker port
    - extra: JSON with optional keys: bootstrap_servers, sasl_mechanism,
             security_protocol, username, password
    """

    conn_name_attr = "kafka_conn_id"
    default_conn_name = "kafka_default"
    conn_type = "kafka"
    hook_name = "Kafka Monitoring"

    def __init__(self, kafka_conn_id: str = "kafka_default", **kwargs):
        super().__init__()
        self.kafka_conn_id = kafka_conn_id
        self._conn = None

    def get_conn(self) -> Dict[str, Any]:
        """Get the Kafka connection configuration."""
        if self._conn is None:
            conn = self.get_connection(self.kafka_conn_id)
            extra = conn.extra_dejson if conn.extra else {}
            host = conn.host or "localhost"
            port = conn.port or 9092

            # If host contains a comma, it's likely a bootstrap_servers string
            if "," in host:
                default_bootstrap = host
            else:
                default_bootstrap = f"{host}:{port}"

            self._conn = {
                "host": host,
                "port": port,
                "bootstrap_servers": extra.get("bootstrap_servers", default_bootstrap),
                "sasl_mechanism": extra.get("sasl_mechanism", "PLAIN"),
                "security_protocol": extra.get("security_protocol", "SASL_PLAINTEXT"),
                "username": conn.login or extra.get("username", ""),
                "password": conn.password or extra.get("password", ""),
            }
        return self._conn

    def check_broker_connectivity(self, timeout: int = 5) -> bool:
        """
        Check if the Kafka broker is reachable via TCP socket.

        Args:
            timeout: Connection timeout in seconds.

        Returns:
            True if broker is reachable, False otherwise.
        """
        conn = self.get_conn()
        bootstrap_servers = conn["bootstrap_servers"]

        for server in bootstrap_servers.split(","):
            server = server.strip()
            if ":" in server:
                host, port_str = server.split(":", 1)
                port = int(port_str)
            else:
                host = server
                port = int(conn["port"])

            self.log.info(f"Checking connectivity to Kafka broker {host}:{port}")
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(timeout)
                try:
                    result = sock.connect_ex((host, port))
                    is_up = result == 0
                except Exception as e:
                    self.log.warning(f"Error checking {host}:{port} - {e}")
                    is_up = False

            if is_up:
                self.log.info(f"Kafka broker {host}:{port} is reachable.")
                return True
            else:
                self.log.warning(f"Kafka broker {host}:{port} is NOT reachable.")

        return False

    def get_cluster_health(self, timeout: int = 5) -> Dict[str, Any]:
        """
        Get comprehensive health info about the Kafka cluster.

        Returns:
            Dictionary with health metrics:
            - broker_reachable: bool
            - host: str
            - port: int
            - response_time_ms: float
        """
        import time

        conn = self.get_conn()
        host = conn["host"]
        port = int(conn["port"])

        start = time.time()
        is_up = self.check_broker_connectivity(timeout)
        elapsed = (time.time() - start) * 1000

        return {
            "broker_reachable": is_up,
            "host": host,
            "port": port,
            "response_time_ms": round(elapsed, 2),
            "bootstrap_servers": conn["bootstrap_servers"],
        }

    def get_consumer_lag(self) -> Dict[str, Any]:
        """
        Estimate consumer group lag.

        Note: Full implementation requires kafka-python or confluent_kafka.
        This is a simplified version that returns connectivity-based status.

        Returns:
            Dictionary with lag info.
        """
        health = self.get_cluster_health()
        return {
            "cluster_healthy": health["broker_reachable"],
            "lag_status": "healthy" if health["broker_reachable"] else "unknown",
            "response_time_ms": health["response_time_ms"],
        }

    def get_message_throughput_estimate(self) -> Dict[str, Any]:
        """
        Provide throughput estimation based on connectivity.

        Returns:
            Throughput status dictionary.
        """
        health = self.get_cluster_health()
        return {
            "cluster_healthy": health["broker_reachable"],
            "throughput_status": "active" if health["broker_reachable"] else "inactive",
            "broker": f"{health['host']}:{health['port']}",
        }
