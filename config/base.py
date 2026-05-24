import os

from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ------------------------------------------------------------------ #
# LOCAL Kafka — dùng cho Spark Structured Streaming                  #
# ------------------------------------------------------------------ #
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

# Confluent producer config cho local Kafka (dùng bởi src/kafka/producer.py)
LOCAL_BOOTSTRAP_SERVERS = os.getenv("LOCAL_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)
LOCAL_TOPIC = os.getenv("LOCAL_TOPIC", KAFKA_TOPIC)
LOCAL_SASL_MECHANISM = os.getenv("LOCAL_SASL_MECHANISM", KAFKA_SASL_MECHANISM)
LOCAL_SECURITY_PROTOCOL = os.getenv("LOCAL_SECURITY_PROTOCOL", KAFKA_SECURITY_PROTOCOL)
LOCAL_SASL_USERNAME = os.getenv("LOCAL_SASL_USERNAME", KAFKA_USERNAME)
LOCAL_SASL_PASSWORD = os.getenv("LOCAL_SASL_PASSWORD", KAFKA_PASSWORD)
LOCAL_GROUP_ID = os.getenv("LOCAL_GROUP_ID", "local-spark-consumer-group")
LOCAL_AUTO_OFFSET_RESET = os.getenv("LOCAL_AUTO_OFFSET_RESET", "earliest")
LOCAL_PRODUCER_ACKS = os.getenv("LOCAL_PRODUCER_ACKS", "all")

# ------------------------------------------------------------------ #
# SERVER Kafka — cụm Kafka remote, nơi dữ liệu gốc đến               #
# ------------------------------------------------------------------ #
SERVER_BOOTSTRAP_SERVERS = os.getenv("SERVER_BOOTSTRAP_SERVERS")
SERVER_TOPIC = os.getenv("SERVER_TOPIC", KAFKA_TOPIC)
SERVER_SASL_MECHANISM = os.getenv("SERVER_SASL_MECHANISM", "PLAIN")
SERVER_SECURITY_PROTOCOL = os.getenv("SERVER_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
SERVER_SASL_USERNAME = os.getenv("SERVER_SASL_USERNAME")
SERVER_SASL_PASSWORD = os.getenv("SERVER_SASL_PASSWORD")
SERVER_GROUP_ID = os.getenv("SERVER_GROUP_ID", "kafka-to-kafka-group")
SERVER_AUTO_OFFSET_RESET = os.getenv("SERVER_AUTO_OFFSET_RESET", "earliest")

# JAAS config string dùng cho Spark Structured Streaming (local Kafka).
if KAFKA_USERNAME and KAFKA_PASSWORD:
    KAFKA_SASL_JAAS_CONFIG = (
        f"org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
    )
else:
    KAFKA_SASL_JAAS_CONFIG = os.getenv("KAFKA_SASL_JAAS_CONFIG", "")

# Confluent-kafka Consumer config để đọc từ Kafka Server (remote).
server_consumer_config = {
    "bootstrap.servers": SERVER_BOOTSTRAP_SERVERS,
    "group.id": SERVER_GROUP_ID,
    "auto.offset.reset": SERVER_AUTO_OFFSET_RESET,
    "enable.auto.commit": False,
}
if SERVER_SECURITY_PROTOCOL:
    server_consumer_config["security.protocol"] = SERVER_SECURITY_PROTOCOL
if SERVER_SASL_MECHANISM:
    server_consumer_config["sasl.mechanism"] = SERVER_SASL_MECHANISM
if SERVER_SASL_USERNAME:
    server_consumer_config["sasl.username"] = SERVER_SASL_USERNAME
if SERVER_SASL_PASSWORD:
    server_consumer_config["sasl.password"] = SERVER_SASL_PASSWORD

# Confluent-kafka Producer config để produce sang Kafka Local.
local_producer_config = {
    "bootstrap.servers": LOCAL_BOOTSTRAP_SERVERS,
    "acks": LOCAL_PRODUCER_ACKS,
}
if LOCAL_SECURITY_PROTOCOL:
    local_producer_config["security.protocol"] = LOCAL_SECURITY_PROTOCOL
if LOCAL_SASL_MECHANISM:
    local_producer_config["sasl.mechanism"] = LOCAL_SASL_MECHANISM
if LOCAL_SASL_USERNAME:
    local_producer_config["sasl.username"] = LOCAL_SASL_USERNAME
if LOCAL_SASL_PASSWORD:
    local_producer_config["sasl.password"] = LOCAL_SASL_PASSWORD

# Postgres
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
sqlalchemy_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Spark
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/spark_checkpoints/user_activity")
IP2LOCATION_DB_PATH = os.getenv(
    "IP2LOCATION_DB_PATH", "/spark/99-project/IP2LOCATION-LITE-DB11.BIN"
)
