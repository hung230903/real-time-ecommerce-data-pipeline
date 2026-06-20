"""
Unit Tests for Custom Operators and Sensors.
"""

import unittest
from unittest.mock import MagicMock, patch

from airflow.models import DAG
from airflow.utils import timezone

from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.spark_job import SparkStreamingOperator

# Import custom components
from plugins.sensors.kafka_sensor import KafkaBrokerSensor

DEFAULT_DATE = timezone.datetime(2025, 1, 1)


class TestKafkaBrokerSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG("test_dag", start_date=DEFAULT_DATE, schedule="@daily")

    @patch("plugins.sensors.kafka_sensor.socket")
    @patch("plugins.sensors.kafka_sensor.KafkaMonitoringHook")
    def test_poke_success(self, mock_hook_cls, mock_socket):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_conn.return_value = {"host": "localhost", "port": 9092}

        mock_sock_instance = MagicMock()
        mock_sock_instance.connect_ex.return_value = 0  # Success
        mock_socket.socket.return_value = mock_sock_instance

        sensor = KafkaBrokerSensor(
            task_id="test_sensor", kafka_conn_id="kafka_test", dag=self.dag
        )

        self.assertTrue(sensor.poke(None))

    @patch("plugins.sensors.kafka_sensor.socket")
    @patch("plugins.sensors.kafka_sensor.KafkaMonitoringHook")
    def test_poke_failure(self, mock_hook_cls, mock_socket):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_conn.return_value = {"host": "localhost", "port": 9092}

        mock_sock_instance = MagicMock()
        mock_sock_instance.connect_ex.return_value = 111  # Failure
        mock_socket.socket.return_value = mock_sock_instance

        sensor = KafkaBrokerSensor(
            task_id="test_sensor", kafka_conn_id="kafka_test", dag=self.dag
        )

        self.assertFalse(sensor.poke(None))


class TestSparkStreamingOperator(unittest.TestCase):
    def test_init(self):
        operator = SparkStreamingOperator(
            task_id="test_spark", kafka_topic="test_topic", project_path="/tmp/project"
        )

        # Check if environment is correctly populated
        self.assertEqual(operator.environment["KAFKA_TOPIC"], "test_topic")
        self.assertEqual(operator.image, "unigap/spark:3.5")
        # Check if command contains the runner path
        self.assertIn("src/streaming/spark_runner.py", operator.command[2])


class TestDataQualityOperator(unittest.TestCase):
    @patch("plugins.operators.data_quality.PostgresExtendedHook")
    def test_execute_pass(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_first.return_value = [10]

        operator = DataQualityOperator(
            task_id="test_dq", sql_checks=[{"sql": "SELECT 1", "expected_result": 10}]
        )

        # Should not raise an exception
        operator.execute(context={"ti": MagicMock()})

    @patch("plugins.operators.data_quality.PostgresExtendedHook")
    def test_execute_fail(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_first.return_value = [5]  # Wrong result

        operator = DataQualityOperator(
            task_id="test_dq", sql_checks=[{"sql": "SELECT 1", "expected_result": 10}]
        )

        with self.assertRaises(ValueError):
            operator.execute(context={"ti": MagicMock()})


if __name__ == "__main__":
    unittest.main()
