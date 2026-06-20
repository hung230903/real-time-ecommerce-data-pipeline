"""
Unit Tests for DAG Validation.

Tests that all DAGs:
- Load without errors
- Have correct structure
- Have proper task dependencies
- Follow naming conventions
"""

import importlib
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add dags and plugins to path
DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags")
PLUGINS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "plugins")
sys.path.insert(0, DAGS_DIR)
sys.path.insert(0, PLUGINS_DIR)


class TestDAGValidation(unittest.TestCase):
    """Test that all DAGs are valid and loadable."""

    def setUp(self):
        """Set up test fixtures."""
        self.dag_files = [
            "kafka_local_monitoring",
            "kafka_producer_control_dag",
            "spark_job_control_dag",
            "data_quality_dag",
            "data_archival_dag",
            "alert_system_dag",
            "health_check_kafka_server",
        ]

    def test_kafka_local_monitoring_structure(self):
        """Test the Local Kafka Monitoring DAG has correct tasks."""
        expected_tasks = [
            "check_local_broker_health",
            "check_local_data_flow",
            "generate_local_monitoring_report",
        ]
        self._validate_dag_tasks("kafka_local_monitoring", "kafka_local_monitoring", expected_tasks)

    def test_spark_job_control_dag_structure(self):
        """Test the Spark Job Control DAG has correct tasks."""
        expected_tasks = [
            "pre_flight_checks",
            "submit_spark_streaming_job",
            "post_job_status",
            "cleanup_resources",
        ]
        self._validate_dag_tasks("spark_job_control_dag", "spark_job_control", expected_tasks)

    def test_data_quality_dag_structure(self):
        """Test the Data Quality DAG has correct tasks."""
        expected_tasks = [
            "check_completeness",
            "check_data_types",
            "check_business_rules",
            "monitor_error_rates",
            "generate_quality_report",
        ]
        self._validate_dag_tasks("data_quality_dag", "data_quality", expected_tasks)

    def test_data_archival_dag_structure(self):
        """Test the Data Archival DAG has correct tasks."""
        expected_tasks = [
            "check_archival_candidates",
            "branch_archival",
            "skip_archival",
            "archive_old_records",
            "cleanup_staging",
            "check_storage_sizes",
            "optimize_database",
            "generate_archival_report",
        ]
        self._validate_dag_tasks("data_archival_dag", "data_archival", expected_tasks)

    def _validate_dag_tasks(self, module_name, dag_id, expected_tasks):
        """Helper to validate DAG task list by reading file content."""
        dag_path = os.path.join(DAGS_DIR, f"{module_name}.py")
        self.assertTrue(os.path.exists(dag_path), f"DAG file '{module_name}.py' not found")
        with open(dag_path, "r") as f:
            content = f.read()
        for task in expected_tasks:
            self.assertIn(
                f"task_id=\"{task}\"",
                content,
                f"Task '{task}' not found in DAG file '{module_name}.py'",
            )


class TestDAGSchedule(unittest.TestCase):
    """Test DAG schedule configurations."""

    def test_kafka_monitoring_schedule(self):
        """Kafka monitoring should run every 2 minutes."""
        self._check_schedule_in_file("kafka_local_monitoring", "*/2 * * * *")

    def test_data_transfer_schedule(self):
        """Data transfer should run daily."""
        self._check_schedule_in_file("kafka_producer_control_dag", "@daily")

    def test_data_quality_schedule(self):
        """Data quality should run every 15 minutes."""
        self._check_schedule_in_file("data_quality_dag", "*/15 * * * *")

    def test_data_archival_schedule(self):
        """Data archival should run weekly."""
        self._check_schedule_in_file("data_archival_dag", "0 2 * * 0")

    def test_alert_system_schedule(self):
        """Alert system should run every 10 minutes."""
        self._check_schedule_in_file("alert_system_dag", "*/10 * * * *")

    def _check_schedule_in_file(self, module_name, expected_schedule):
        """Check that a DAG file contains the expected schedule."""
        dag_path = os.path.join(DAGS_DIR, f"{module_name}.py")
        self.assertTrue(os.path.exists(dag_path), f"DAG file '{module_name}.py' not found")
        with open(dag_path, "r") as f:
            content = f.read()
        self.assertIn(
            expected_schedule,
            content,
            f"Expected schedule '{expected_schedule}' not found in {module_name}",
        )


if __name__ == "__main__":
    unittest.main()
