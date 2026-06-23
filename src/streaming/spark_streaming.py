import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    from_unixtime,
    lit,
    sha2,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType

import config.base as settings
from config.logger import setup_logger
from src.load.db_upserter import upsert_dimensions_partition

logger = setup_logger("SparkStreaming")

# Lookup DF schema
DIM_SCHEMA = StructType(
    [
        StructField("map_id", StringType(), True),
        StructField("map_key", StringType(), True),
    ]
)


######################################################################
# 1. DIMENSION UPSERTS  (Fix #1 – worker-side via foreachPartition)
######################################################################
def upsert_all_dimensions(batch_df):
    """
    Push dimension upserts down to Spark workers via foreachPartition.

    Each partition opens its own Postgres connection, deduplicates locally,
    executes all INSERTs, and commits exactly once – no data is pulled back
    to the driver (no collect()), and there are no per-row commits.
    """
    dim_df = batch_df.select(
        "product_id",
        "store_id",
        col("loc_info.location_id").alias("location_id"),
        col("loc_info.country_name").alias("country_name"),
        col("loc_info.country_short").alias("country_short"),
        col("loc_info.region_name").alias("region_name"),
        col("loc_info.city_name").alias("city_name"),
        "device_id",
        "email_address",
        "user_id_db",
        "user_agent",
        "resolution",
        "time_stamp",
    )

    # Deduplicate on the Spark side before shipping to workers to further
    # reduce redundant DB calls across partitions.
    dim_df = dim_df.distinct()

    dim_df.foreachPartition(upsert_dimensions_partition)


######################################################################
# 2. LOAD DIMENSION MAPS from DB (driver queries PG for surrogate keys)
######################################################################
def _pg_conn():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )


def load_dimension_maps(batch_df):
    """
    After workers have upserted all dimensions, fetch the surrogate-key
    maps from Postgres on the driver for the *values present in this batch*.

    This keeps the driver query small (WHERE IN <batch values>) instead of
    scanning entire dimension tables.
    """
    # Collect only the natural keys – these are tiny sets compared to the
    # full row payloads, so collect() here is safe.
    product_ids = [
        r.product_id
        for r in batch_df.select("product_id").distinct().collect()
        if r.product_id
    ]

    store_ids = [
        r.store_id
        for r in batch_df.select("store_id").distinct().collect()
        if r.store_id
    ]

    location_ids = [
        r.location_id
        for r in batch_df.select(col("loc_info.location_id").alias("location_id"))
        .distinct()
        .collect()
        if r.location_id
    ]

    device_ids = [
        r.device_id
        for r in batch_df.select("device_id").distinct().collect()
        if r.device_id
    ]

    conn = _pg_conn()
    cur = conn.cursor()

    def fetch_map(query, ids):
        if not ids:
            return {}
        cur.execute(query, (ids,))
        return {row[0]: str(row[1]) for row in cur.fetchall()}

    product_map = fetch_map(
        "SELECT product_id, product_key FROM dim_product WHERE product_id = ANY(%s)",
        product_ids,
    )
    store_map = fetch_map(
        "SELECT store_id, store_key FROM dim_store WHERE store_id = ANY(%s)",
        store_ids,
    )
    location_map = fetch_map(
        "SELECT location_id, location_key FROM dim_location WHERE location_id = ANY(%s)",
        location_ids,
    )

    # Customer map: keyed by customer_id (= device_id after transformation)
    if device_ids:
        cur.execute(
            "SELECT customer_id, customer_key FROM dim_customer WHERE customer_id = ANY(%s)",
            (device_ids,),
        )
        customer_map = {row[0]: str(row[1]) for row in cur.fetchall()}
    else:
        customer_map = {}

    # Device map: keyed by device_id (SHA-256 hash)
    if device_ids:
        cur.execute(
            "SELECT device_id, device_key FROM dim_device WHERE device_id = ANY(%s)",
            (device_ids,),
        )
        device_map = {row[0]: str(row[1]) for row in cur.fetchall()}
    else:
        device_map = {}

    # Date map: keyed by date_id (YYYYMMDD int as string)
    # dim_date has no surrogate key – date_id IS the key, so we map it
    # to itself (identity mapping) for consistency with other dimensions.
    cur.execute("SELECT date_id, date_id FROM dim_date")
    date_map = {str(row[0]): str(row[1]) for row in cur.fetchall()}

    cur.close()
    conn.close()

    return product_map, store_map, location_map, customer_map, date_map, device_map


######################################################################
# 3. ENRICH BATCH (join batch_df with dimension maps → create fact_id)
######################################################################
def create_lookup_df(spark, mapping, id_col, key_col):
    """Dict → 2-column Spark DataFrame to join"""
    data = [(str(k), str(v) if v else None) for k, v in mapping.items()]
    return (
        spark.createDataFrame(data, schema=DIM_SCHEMA)
        .withColumnRenamed("map_id", id_col)
        .withColumnRenamed("map_key", key_col)
    )


def build_enriched_df(
    batch_df, product_map, store_map, location_map, customer_map, date_map, device_map
):
    """
    Join batch_df with lookup DataFrames, generate fact_id with SHA-256,
    and select columns for fact_product_views.
    """
    spark = SparkSession.builder.getOrCreate()

    # Handle timestamp in milliseconds (if > 10^10, assume ms)
    ts_col = when(col("time_stamp") > 9999999999, col("time_stamp") / 1000.0).otherwise(
        col("time_stamp")
    )

    prod_df = create_lookup_df(spark, product_map, "map_prod_id", "product_key")
    store_df = create_lookup_df(spark, store_map, "map_store_id", "store_key")
    loc_df = create_lookup_df(spark, location_map, "map_loc_id", "loc_key")
    cus_df = create_lookup_df(spark, customer_map, "map_cus_id", "cus_key")
    dev_df = create_lookup_df(spark, device_map, "map_dev_id", "dev_key")
    date_df = create_lookup_df(spark, date_map, "date_id_str", "date_key")

    enriched_df = (
        batch_df.join(prod_df, batch_df.product_id == prod_df.map_prod_id, "left")
        .join(store_df, batch_df.store_id == store_df.map_store_id, "left")
        .join(loc_df, batch_df.loc_info.location_id == loc_df.map_loc_id, "left")
        .join(cus_df, batch_df.device_id == cus_df.map_cus_id, "left")
        .withColumn(
            "device_id_hash",
            sha2(concat_ws("_", col("user_agent"), col("resolution")), 256),
        )
        .join(dev_df, col("device_id_hash") == dev_df.map_dev_id, "left")
        .join(
            date_df,
            date_format(from_unixtime(ts_col), "yyyyMMdd") == date_df.date_id_str,
            "left",
        )
        .withColumn(
            "fact_id",
            sha2(
                concat_ws(
                    "_",
                    coalesce(col("product_key"), lit("NA")),
                    coalesce(col("store_key"), lit("NA")),
                    coalesce(col("loc_key"), lit("NA")),
                    coalesce(col("cus_key"), lit("NA")),
                    coalesce(col("dev_key"), lit("NA")),
                    coalesce(col("date_key"), lit("NA")),
                    col("time_stamp").cast("string"),
                ),
                256,
            ),
        )
        .select(
            col("fact_id"),
            col("product_key").cast("integer").alias("product_key"),
            col("store_key").cast("integer").alias("store_key"),
            col("loc_key").cast("integer").alias("location_key"),
            col("cus_key").cast("integer").alias("customer_key"),
            col("dev_key").cast("integer").alias("device_key"),
            col("date_key").cast("integer").alias("date_id"),
            col("ip").alias("ip_address"),
            col("collection"),
            col("current_url"),
            col("referrer_url"),
            from_unixtime(ts_col).cast("timestamp").alias("time_stamp"),
        )
    )
    return enriched_df


######################################################################
# 4. FACT UPSERT
######################################################################
def write_fact_table(enriched_df):
    pg_host = settings.POSTGRES_HOST
    pg_port = settings.POSTGRES_PORT
    pg_db = settings.POSTGRES_DB
    pg_user = settings.POSTGRES_USER
    pg_pass = settings.POSTGRES_PASSWORD

    def upsert_fact_partition(rows):
        import psycopg2 as _pg
        from psycopg2.extras import execute_batch

        sql = """
              INSERT INTO fact_product_views (fact_id, product_key, store_key, location_key,
                                              customer_key, device_key, date_id, ip_address, time_stamp,
                                              collection, current_url, referrer_url)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
              ON CONFLICT (fact_id) DO NOTHING
              """

        conn = _pg.connect(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_pass,
        )
        cur = conn.cursor()

        batch = [
            (
                r.fact_id,
                r.product_key,
                r.store_key,
                r.location_key,
                r.customer_key,
                r.device_key,
                r.date_id,
                r.ip_address,
                r.time_stamp,
                r.collection,
                r.current_url,
                r.referrer_url,
            )
            for r in rows
        ]

        if batch:
            execute_batch(cur, sql, batch, page_size=500)
            conn.commit()

        cur.close()
        conn.close()

    enriched_df.printSchema()
    enriched_df.foreachPartition(upsert_fact_partition)


######################################################################
# ORCHESTRATOR
######################################################################
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"--- Batch {batch_id} is empty, skipping ---")
        return

    logger.info(f"--- Processing batch {batch_id} ---")

    # Add batch_df to RAM
    batch_df.cache()

    count_raw = batch_df.count()
    logger.info(f"Batch {batch_id} has {count_raw} raw events")

    # Fix #1: push dimension upserts to workers via foreachPartition –
    # no collect(), no driver OOM, fully distributed.
    upsert_all_dimensions(batch_df)

    # Fetch surrogate-key maps from PG on the driver (key-only, small payload)
    product_map, store_map, location_map, customer_map, date_map, device_map = (
        load_dimension_maps(batch_df)
    )

    # Enrich batch with dimension keys
    enriched_df = build_enriched_df(
        batch_df,
        product_map,
        store_map,
        location_map,
        customer_map,
        date_map,
        device_map,
    )

    count_enriched = enriched_df.count()

    # Upsert data to fact table
    logger.info(f"Upserting {count_enriched} rows into fact_product_views")
    write_fact_table(enriched_df)

    # Remove batch_df from RAM after processed
    batch_df.unpersist()
    logger.info(f"Batch {batch_id} completed.")
