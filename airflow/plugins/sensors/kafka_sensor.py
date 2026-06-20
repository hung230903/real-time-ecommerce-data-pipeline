"""
Custom Kafka Sensors for Airflow.

Enhanced version with 'reschedule' support and multi-broker failover.
"""

import socket
from contextlib import closing

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from plugins.hooks.kafka_hook import KafkaMonitoringHook


class KafkaBrokerSensor(BaseSensorOperator):
    """
    Enhanced Sensor that waits for Kafka broker availability.
    Supports multi-broker lists and 'reschedule' mode to save resources.
    """

    template_fields = ("kafka_conn_id", "bootstrap_servers")
    ui_color = "#e8f7e4"

    def __init__(
        self,
        kafka_conn_id: str = "kafka_default",
        bootstrap_servers: str = None,
        conn_timeout: int = 5,
        mode: str = "reschedule",  # Release worker slot between pokes
        **kwargs,
    ):
        super().__init__(mode=mode, **kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.bootstrap_servers = bootstrap_servers
        self.conn_timeout = conn_timeout

    def poke(self, context: Context) -> bool:
        """
        Check if at least one Kafka broker is reachable.
        """
        hook = KafkaMonitoringHook(kafka_conn_id=self.kafka_conn_id)

        # Determine targets
        targets = []
        if self.bootstrap_servers:
            import re

            # Clean up potential "Val (Value): " prefix from Airflow UI copy-paste
            clean_servers = re.sub(
                r"^Val\s*\(Value\):\s*", "", self.bootstrap_servers, flags=re.IGNORECASE
            ).strip()

            # Handle multiple brokers in a comma-separated list
            for server in clean_servers.split(","):
                server = server.strip()
                if not server:
                    continue
                parts = server.split(":")
                host = parts[0].strip()
                port = int(parts[1].strip()) if len(parts) > 1 else 9092
                if host:
                    targets.append((host, port))
        else:
            conn = hook.get_conn()
            targets.append((conn["host"], int(conn["port"])))

        self.log.info(f"Checking connectivity for {len(targets)} potential brokers...")

        # Try brokers one by one (Failover logic)
        for host, port in targets:
            try:
                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                    sock.settimeout(self.conn_timeout)
                    if sock.connect_ex((host, port)) == 0:
                        self.log.info(f"✅ Reachable: {host}:{port}")
                        return True
                    else:
                        self.log.warning(f"⏳ Unreachable: {host}:{port}")
            except Exception as e:
                self.log.error(f"💥 Connection attempt failed for {host}:{port}: {e}")

        self.log.error("❌ None of the brokers are currently reachable.")
        return False
