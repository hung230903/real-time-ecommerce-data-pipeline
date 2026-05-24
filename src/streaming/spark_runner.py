import os
import zipfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf

from config.base import (
    IP2LOCATION_DB_PATH,
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_JAAS_CONFIG,
)
from config.logger import setup_logger
from src.processing.ip_enricher import get_loc_info  # noqa: F401 – used in ip_loc_enricher UDF lambda
from src.schema.schemas import EVENT_SCHEMA, LOCATION_SCHEMA
from src.streaming.spark_streaming import process_batch

logger = setup_logger(name="SparkDriver", log_file="driver.log")


###############################
# Package helpers
###############################
def package_dir(src_dir, zip_path):
    """Zip all .py files under *src_dir* into *zip_path*"""
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for f in files:
                if f.endswith(".py"):
                    zf.write(os.path.join(root, f))
    return zip_path


###############################
# IP-location UDF
###############################
_IP2LOC_PATH = IP2LOCATION_DB_PATH

ip_loc_enricher = udf(
    lambda ip: get_loc_info(ip, _IP2LOC_PATH),
    LOCATION_SCHEMA,
)


###############################
# Main
###############################
def main():
    spark = (
        SparkSession.builder.appName("RealtimeStarSchemaAnalysis")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Distribute IP2Location binary to workers
    spark.sparkContext.addFile(IP2LOCATION_DB_PATH)

    # Distribute project source code to workers with unique name to avoid cache issues
    import time

    ts_zip = int(time.time())
    src_zip = f"/tmp/src_{ts_zip}.zip"
    spark.sparkContext.addPyFile(package_dir("src", src_zip))

    config_zip = "/tmp/config.zip"
    with zipfile.ZipFile(config_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("config/base.py", arcname="config/base.py")
        zf.write("config/logger.py", arcname="config/logger.py")
        zf.write("config/__init__.py", arcname="config/__init__.py")
    spark.sparkContext.addPyFile(config_zip)

    if os.path.exists(".env"):
        spark.sparkContext.addFile(".env")

    # Read from Kafka
    logger.info(
        f"Subscribing to Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}"
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON + enrich with IP location
    parsed_stream = (
        raw_stream.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
        )
        .select("data.*")
        .withColumn("loc_info", ip_loc_enricher(col("ip")))
    )

    # Write foreachBatch
    query = (
        parsed_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
