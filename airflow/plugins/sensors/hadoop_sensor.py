"""
Custom Hadoop Sensors for Airflow.

Checks that critical Hadoop services (HDFS NameNode and YARN
ResourceManager) are reachable before submitting Spark-on-YARN jobs.
"""

import socket
from contextlib import closing
from typing import List, Tuple

from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class HadoopClusterSensor(BaseSensorOperator):
    """
    Sensor that waits for Hadoop cluster availability.

    Checks connectivity to both the HDFS NameNode and the YARN
    ResourceManager.  The Spark job requires both services to be up
    before submission.

    Parameters
    ----------
    hadoop_conn_id : str
        Airflow connection ID whose *host* field points to the NameNode
        (default ``hadoop_default``).  If *namenode_host* /
        *resourcemanager_host* are supplied they take precedence.
    namenode_host : str
        NameNode hostname (default ``namenode``).
    namenode_port : int
        NameNode **RPC** port (default ``9000``).
    resourcemanager_host : str
        YARN ResourceManager hostname (default ``resourcemanager``).
    resourcemanager_port : int
        ResourceManager scheduler port (default ``8088``).
    conn_timeout : int
        TCP socket timeout in seconds (default ``5``).
    check_both : bool
        If ``True`` (default), *both* NameNode and ResourceManager must
        be reachable.  If ``False``, at least one must be reachable.
    """

    template_fields = (
        "namenode_host",
        "namenode_port",
        "resourcemanager_host",
        "resourcemanager_port",
    )
    ui_color = "#fce4b8"  # warm amber – visually distinct from Kafka's green

    def __init__(
        self,
        hadoop_conn_id: str = "hadoop_default",
        namenode_host: str = None,
        namenode_port: int = 8020,
        resourcemanager_host: str = None,
        resourcemanager_port: int = 8088,
        conn_timeout: int = 5,
        check_both: bool = True,
        mode: str = "reschedule",
        **kwargs,
    ):
        super().__init__(mode=mode, **kwargs)
        self.hadoop_conn_id = hadoop_conn_id
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.resourcemanager_host = resourcemanager_host
        self.resourcemanager_port = resourcemanager_port
        self.conn_timeout = conn_timeout
        self.check_both = check_both

    # ── helpers ──────────────────────────────────────────────────────

    def _resolve_targets(self) -> List[Tuple[str, str, int]]:
        """Return ``[(label, host, port), ...]`` for every service to check."""
        targets: List[Tuple[str, str, int]] = []

        nn_host = self.namenode_host
        rm_host = self.resourcemanager_host

        # Fall back to Airflow connection if explicit hosts are not set
        if not nn_host or not rm_host:
            try:
                conn = BaseHook.get_connection(self.hadoop_conn_id)
                if not nn_host:
                    nn_host = conn.host or "namenode"
                if not rm_host:
                    rm_host = conn.extra_dejson.get(
                        "resourcemanager_host", "resourcemanager"
                    )
            except Exception:
                nn_host = nn_host or "namenode"
                rm_host = rm_host or "resourcemanager"

        targets.append(("HDFS NameNode", nn_host, self.namenode_port))
        targets.append(("YARN ResourceManager", rm_host, self.resourcemanager_port))
        return targets

    @staticmethod
    def _tcp_check(host: str, port: int, timeout: int) -> bool:
        """Return ``True`` if a TCP connection to *host:port* succeeds."""
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(timeout)
                return sock.connect_ex((host, port)) == 0
        except Exception:
            return False

    # ── sensor interface ─────────────────────────────────────────────

    def poke(self, context: Context) -> bool:
        """Check if Hadoop services are reachable."""
        targets = self._resolve_targets()
        results = {}

        for label, host, port in targets:
            ok = self._tcp_check(host, port, self.conn_timeout)
            results[label] = ok
            if ok:
                self.log.info(f"✅ {label} reachable at {host}:{port}")
            else:
                self.log.warning(f"⏳ {label} unreachable at {host}:{port}")

        if self.check_both:
            all_ok = all(results.values())
            if not all_ok:
                failed = [k for k, v in results.items() if not v]
                self.log.error(
                    f"❌ Hadoop pre-flight failed – unreachable: {', '.join(failed)}"
                )
            return all_ok

        any_ok = any(results.values())
        if not any_ok:
            self.log.error("❌ None of the Hadoop services are currently reachable.")
        return any_ok
