from unittest.mock import MagicMock, patch

import pytest

from src.kafka.producer import delivery_report


def test_delivery_report_success():
    # Arrange
    msg_count_holder = [0]
    produced_msg = MagicMock()
    produced_msg.topic.return_value = "test-topic"
    produced_msg.partition.return_value = 0
    produced_msg.offset.return_value = 100

    # Act
    with patch("src.kafka.producer.logger.info") as mock_info:
        delivery_report(None, produced_msg, msg_count_holder)

    # Assert
    assert msg_count_holder[0] == 1
    mock_info.assert_called_once()
    assert "Forwarded → test-topic" in mock_info.call_args[0][0]


def test_delivery_report_failure():
    # Arrange
    msg_count_holder = [0]
    produced_msg = MagicMock()
    err = "Test Error"

    # Act
    with patch("src.kafka.producer.logger.error") as mock_error:
        delivery_report(err, produced_msg, msg_count_holder)

    # Assert
    assert msg_count_holder[0] == 0  # Should not increment
    mock_error.assert_called_once_with("Delivery failed: Test Error")


# For testing the run_producer main loop we would need to mock heavily
# due to the infinite loop, Consumer, and Producer interactions.
# Here is an example of testing initialization failure:
@patch("src.kafka.producer.Consumer")
@patch("src.kafka.producer.Producer")
def test_run_producer_initialization_failure(mock_producer, mock_consumer):
    # Arrange
    mock_consumer.side_effect = Exception("Consumer init failed")

    from src.kafka.producer import run_producer

    # Act & Assert
    with patch("sys.exit") as mock_exit:
        mock_exit.side_effect = SystemExit(1)
        with patch("src.kafka.producer.logger.error") as mock_logger_error:
            with pytest.raises(SystemExit):
                run_producer()

            mock_logger_error.assert_called_with(
                "Failed to initialize Kafka clients or subscribe: Consumer init failed"
            )
            mock_exit.assert_called_once_with(1)
