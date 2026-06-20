"""
Unit Tests for Custom Hooks.

Tests the logic of custom hooks:
- KafkaMonitoringHook
- PostgresExtendedHook
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add plugins to path
PLUGINS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "plugins")
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, PLUGINS_DIR)


class TestKafkaMonitoringHook(unittest.TestCase):
    """Test KafkaMonitoringHook methods."""

    @patch("plugins.hooks.kafka_hook.KafkaMonitoringHook.get_connection")
    def test_get_conn(self, mock_get_conn):
        """Test connection configuration retrieval."""
        mock_conn = MagicMock()
        mock_conn.host = "kafka-broker"
        mock_conn.port = 9092
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.extra = '{"bootstrap_servers": "kafka-broker:9092"}'
        mock_conn.extra_dejson = {"bootstrap_servers": "kafka-broker:9092"}
        mock_get_conn.return_value = mock_conn

        from plugins.hooks.kafka_hook import KafkaMonitoringHook

        hook = KafkaMonitoringHook(kafka_conn_id="test_kafka")
        conn = hook.get_conn()

        self.assertEqual(conn["host"], "kafka-broker")
        self.assertEqual(conn["port"], 9092)
        self.assertEqual(conn["bootstrap_servers"], "kafka-broker:9092")

    @patch("plugins.hooks.kafka_hook.KafkaMonitoringHook.get_connection")
    @patch("socket.socket")
    def test_check_broker_connectivity_up(self, mock_socket_cls, mock_get_conn):
        """Test broker connectivity when broker is up."""
        mock_conn = MagicMock()
        mock_conn.host = "localhost"
        mock_conn.port = 9092
        mock_conn.extra = "{}"
        mock_conn.extra_dejson = {}
        mock_conn.login = None
        mock_conn.password = None
        mock_get_conn.return_value = mock_conn

        mock_socket = MagicMock()
        mock_socket.connect_ex.return_value = 0
        mock_socket.__enter__ = MagicMock(return_value=mock_socket)
        mock_socket.__exit__ = MagicMock(return_value=False)
        mock_socket_cls.return_value = mock_socket

        from plugins.hooks.kafka_hook import KafkaMonitoringHook

        hook = KafkaMonitoringHook(kafka_conn_id="test_kafka")
        result = hook.check_broker_connectivity(timeout=1)

        self.assertTrue(result)

    @patch("plugins.hooks.kafka_hook.KafkaMonitoringHook.get_connection")
    @patch("socket.socket")
    def test_check_broker_connectivity_down(self, mock_socket_cls, mock_get_conn):
        """Test broker connectivity when broker is down."""
        mock_conn = MagicMock()
        mock_conn.host = "localhost"
        mock_conn.port = 9092
        mock_conn.extra = "{}"
        mock_conn.extra_dejson = {}
        mock_conn.login = None
        mock_conn.password = None
        mock_get_conn.return_value = mock_conn

        mock_socket = MagicMock()
        mock_socket.connect_ex.return_value = 1  # Connection refused
        mock_socket.__enter__ = MagicMock(return_value=mock_socket)
        mock_socket.__exit__ = MagicMock(return_value=False)
        mock_socket_cls.return_value = mock_socket

        from plugins.hooks.kafka_hook import KafkaMonitoringHook

        hook = KafkaMonitoringHook(kafka_conn_id="test_kafka")
        result = hook.check_broker_connectivity(timeout=1)

        self.assertFalse(result)

    @patch("plugins.hooks.kafka_hook.KafkaMonitoringHook.get_connection")
    @patch("socket.socket")
    def test_get_cluster_health(self, mock_socket_cls, mock_get_conn):
        """Test cluster health report generation."""
        mock_conn = MagicMock()
        mock_conn.host = "localhost"
        mock_conn.port = 9092
        mock_conn.extra = "{}"
        mock_conn.extra_dejson = {}
        mock_conn.login = None
        mock_conn.password = None
        mock_get_conn.return_value = mock_conn

        mock_socket = MagicMock()
        mock_socket.connect_ex.return_value = 0
        mock_socket.__enter__ = MagicMock(return_value=mock_socket)
        mock_socket.__exit__ = MagicMock(return_value=False)
        mock_socket_cls.return_value = mock_socket

        from plugins.hooks.kafka_hook import KafkaMonitoringHook

        hook = KafkaMonitoringHook(kafka_conn_id="test_kafka")
        health = hook.get_cluster_health(timeout=1)

        self.assertIn("broker_reachable", health)
        self.assertIn("response_time_ms", health)
        self.assertTrue(health["broker_reachable"])


class TestPostgresExtendedHook(unittest.TestCase):
    """Test PostgresExtendedHook methods."""

    @patch("plugins.hooks.postgres_hook_ext.PostgresExtendedHook.get_first")
    def test_check_record_count(self, mock_get_first):
        """Test record count retrieval."""
        mock_get_first.return_value = [42]

        from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

        hook = PostgresExtendedHook.__new__(PostgresExtendedHook)
        result = hook.check_record_count("test_table")

        self.assertEqual(result, 42)
        mock_get_first.assert_called_once_with("SELECT COUNT(*) FROM test_table")

    @patch("plugins.hooks.postgres_hook_ext.PostgresExtendedHook.get_first")
    def test_check_duplicate_count(self, mock_get_first):
        """Test duplicate count detection."""
        mock_get_first.return_value = [5]

        from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

        hook = PostgresExtendedHook.__new__(PostgresExtendedHook)
        result = hook.check_duplicate_count("test_table", "id")

        self.assertEqual(result, 5)

    @patch("plugins.hooks.postgres_hook_ext.PostgresExtendedHook.get_first")
    @patch("plugins.hooks.postgres_hook_ext.PostgresExtendedHook.get_records")
    def test_check_table_completeness(self, mock_get_records, mock_get_first):
        """Test table completeness check."""
        # Row count
        mock_get_first.side_effect = [[100], [5], [0]]  # total, null_col1, null_col2

        # Column names
        mock_get_records.return_value = [("col1",), ("col2",)]

        from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

        hook = PostgresExtendedHook.__new__(PostgresExtendedHook)
        result = hook.check_table_completeness("test_table")

        self.assertEqual(result["table"], "test_table")
        self.assertEqual(result["total_rows"], 100)
        self.assertIn("completeness_pct", result)


if __name__ == "__main__":
    unittest.main()
