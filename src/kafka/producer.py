from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config.base import (
    COMMIT_BATCH,
    LOCAL_BOOTSTRAP_SERVERS,
    LOCAL_PRODUCER_CONFIG,
    LOCAL_TOPIC,
    SERVER_BOOTSTRAP_SERVERS,
    SERVER_CONSUMER_CONFIG,
    SERVER_TOPIC,
)
from config.logger import setup_logger

logger = setup_logger(name="KafkaProducer", log_folder="kafka", log_file="producer.log")


def delivery_report(err, produced_msg, msg_count_holder):
    """
    Callback on produce success/failure — DO NOT commit offset here.
    This callback is NOT invoked automatically by the background thread.
    It is only triggered when the main Python thread explicitly calls `producer.poll()`,
    fetching the delivery results from the background C/C++ thread's event queue.
    """
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        msg_count_holder[0] += 1
        count = msg_count_holder[0]
        logger.info(
            f"[#{count}] Forwarded → {produced_msg.topic()} "
            f"[partition={produced_msg.partition()}, offset={produced_msg.offset()}]"
        )


def run_producer():
    try:
        # Initialize Consumer and Producer
        consumer = Consumer(SERVER_CONSUMER_CONFIG)
        producer = Producer(LOCAL_PRODUCER_CONFIG)

        # Subscribe topic for Consumer
        consumer.subscribe([SERVER_TOPIC])
        logger.info(
            f"Kafka producer started: [{SERVER_TOPIC}] "
            f"({SERVER_BOOTSTRAP_SERVERS}) → "
            f"[{LOCAL_TOPIC}] ({LOCAL_BOOTSTRAP_SERVERS})"
        )
    except Exception as e:
        import sys

        logger.error(f"Failed to initialize Kafka clients or subscribe: {e}")
        sys.exit(1)

    msg_count_holder = [0]
    pending_commit = 0  # Number of uncommitted messages

    try:
        while True:
            # Poll data to Server
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Still commit if there is pending (during idle)
                if pending_commit > 0:
                    try:
                        consumer.commit(asynchronous=False)
                        pending_commit = 0
                    except KafkaException as e:
                        logger.error(f"Offset commit failed: {e}")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                # Push the message into the in-memory RAM buffer.
                # The background C/C++ thread will automatically take data from the buffer and send it over the network.
                # This function does NOT wait for the transmission to complete.
                producer.produce(
                    LOCAL_TOPIC,
                    value=msg.value(),
                    on_delivery=lambda err, p_msg: delivery_report(
                        err, p_msg, msg_count_holder
                    ),
                )
                pending_commit += 1
            except BufferError:
                logger.warning("Producer queue is full, flushing and retrying...")

                # The RAM buffer is full. Block the main thread for up to 1s to allow the background C/C++ thread
                # to send pending data and free up space, before continuing the loop to enqueue new messages.
                producer.poll(1)
                continue
            except Exception as e:
                logger.error(f"Produce error: {e}")

            # Fetch delivery results from the background C/C++ thread's event queue.
            # If any messages have been successfully sent or failed, this triggers the `delivery_report` callback.
            # Timeout = 0 means "non-blocking", it checks and immediately returns so the while loop isn't delayed.
            producer.poll(0)

            # Commit offset every _COMMIT_INTERVAL messages
            if pending_commit >= COMMIT_BATCH:
                # BLOCKING CALL! Force the background thread to send all pending data in the RAM buffer.
                # Wait here until all messages in the batch are physically delivered to the Local Kafka broker.
                producer.flush()  # Ensure all messages are sent
                try:
                    # Once we are absolutely sure the messages are safely stored locally ,
                    # we can now safely report back to the Server Kafka that we have processed them.
                    # This guarantees At-Least-Once delivery semantics and prevents data loss.
                    consumer.commit(asynchronous=False)
                    pending_commit = 0
                except KafkaException as e:
                    logger.error(f"Offset commit failed: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        logger.info(f"Total messages forwarded: {msg_count_holder[0]}")
        logger.info("Flushing producer and closing consumer...")
        producer.flush(timeout=10)
        # Final commit before closing
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass
        consumer.close()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    run_producer()
