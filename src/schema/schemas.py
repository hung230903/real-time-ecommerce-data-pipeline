from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

OPTION_SCHEMA = StructType(
    [
        StructField("option_id", StringType()),
        StructField("option_label", StringType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("api_version", StringType()),
        StructField("collection", StringType()),
        StructField("current_url", StringType()),
        StructField("device_id", StringType()),
        StructField("user_id_db", StringType()),
        StructField("resolution", StringType()),
        StructField("email_address", StringType()),
        StructField("ip", StringType()),
        StructField("local_time", StringType()),
        StructField("option", ArrayType(OPTION_SCHEMA)),
        StructField("product_id", StringType()),
        StructField("referrer_url", StringType()),
        StructField("store_id", StringType()),
        StructField("time_stamp", LongType()),
        StructField("user_agent", StringType()),
    ]
)

LOCATION_SCHEMA = StructType(
    [
        StructField("location_id", StringType()),
        StructField("country_name", StringType()),
        StructField("country_short", StringType()),
        StructField("region_name", StringType()),
        StructField("city_name", StringType()),
    ]
)
