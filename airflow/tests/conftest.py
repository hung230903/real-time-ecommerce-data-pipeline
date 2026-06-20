"""
Pytest configuration and fixtures.
"""

import os
import sys

import pytest

# Add project paths
ROOT_DIR = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(ROOT_DIR, "dags"))
sys.path.insert(0, os.path.join(ROOT_DIR, "plugins"))
sys.path.insert(0, ROOT_DIR)


@pytest.fixture
def mock_postgres_hook():
    """Provide a mocked PostgresExtendedHook."""
    from unittest.mock import MagicMock

    hook = MagicMock()
    hook.get_staging_count.return_value = 0
    hook.get_fact_table_count.return_value = 0
    hook.check_record_count.return_value = 0
    return hook


@pytest.fixture
def mock_kafka_hook():
    """Provide a mocked KafkaMonitoringHook."""
    from unittest.mock import MagicMock

    hook = MagicMock()
    hook.check_broker_connectivity.return_value = True
    hook.get_cluster_health.return_value = {
        "broker_reachable": True,
        "host": "localhost",
        "port": 9092,
        "response_time_ms": 5.0,
        "bootstrap_servers": "localhost:9092",
    }
    return hook
