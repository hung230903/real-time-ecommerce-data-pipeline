import logging

import psycopg2
from psycopg2.extras import execute_batch

import config.base as settings
from src.processing.data_transformer import (
    customer_transformer,
    date_transformer,
    device_transformer,
    parse_user_agent,
    store_transformer,
)


# ---------------------------------------------------------------------------
# Low-level upsert helpers – execute SQL but do NOT commit.
# The caller is responsible for committing (or rolling back) the transaction.
# ---------------------------------------------------------------------------

def upsert_location_dimension(cur, values):
    sql = """
          INSERT INTO dim_location (location_id, country_name, country_short, region_name, city_name)
          VALUES (%s, %s, %s, %s, %s) ON CONFLICT (location_id) DO
          UPDATE SET location_id = EXCLUDED.location_id
              RETURNING location_id;
          """
    cur.execute(sql, values)
    result = cur.fetchone()
    return result[0] if result else None


def upsert_product_dimension(cur, values):
    sql = """
          INSERT INTO dim_product (product_id)
          VALUES (%s) ON CONFLICT (product_id) DO
          UPDATE SET product_id = EXCLUDED.product_id
              RETURNING product_id;
          """
    cur.execute(sql, values)
    result = cur.fetchone()
    return result[0] if result else None


def upsert_store_dimension(cur, values):
    sql = """
          INSERT INTO dim_store (store_id, store_name)
          VALUES (%s, %s) ON CONFLICT (store_id) DO
          UPDATE SET store_id = EXCLUDED.store_id RETURNING store_id;
          """
    cur.execute(sql, values)
    result = cur.fetchone()
    return result[0] if result else None


def upsert_date_dimension(cur, values):
    sql = """
          INSERT INTO dim_date (date_id, full_date, date_of_week, date_of_week_short,
                                is_weekday_or_weekend, day_of_month, day_of_year,
                                week_of_year, quarter_number, year_number, year_month)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT (date_id) DO NOTHING RETURNING date_id;
          """
    cur.execute(sql, values)
    result = cur.fetchone()
    return result[0] if result else None


def upsert_device_dimension(cur, values):
    sql = """
          INSERT INTO dim_device (device_id, user_agent, resolution, os, browser)
          VALUES (%s, %s, %s, %s, %s) ON CONFLICT (device_id) DO NOTHING RETURNING device_id;
          """
    cur.execute(sql, values)
    result = cur.fetchone()
    return result[0] if result else None


def upsert_customer_dimension(cur, values):
    sql = """
          INSERT INTO dim_customer (customer_id, email_address, user_id_db)
          VALUES (%s, %s, %s) ON CONFLICT (customer_id) DO
          UPDATE SET email_address = EXCLUDED.email_address,
                     user_id_db    = EXCLUDED.user_id_db
              RETURNING customer_id;
          """
    cur.execute(sql, values)
    result = cur.fetchone()
    return result[0] if result else None


# ---------------------------------------------------------------------------
# Worker-side partition handler (Fix #1 + Fix #4)
#
# Called via DataFrame.foreachPartition().  Each worker opens its own
# connection, processes every row in the partition, and commits exactly once
# at the end – eliminating both the driver-side collect() and the per-row
# commit overhead.
# ---------------------------------------------------------------------------

def upsert_dimensions_partition(rows):
    """
    Process a Spark partition entirely on the worker node.

    - Opens a single Postgres connection per partition.
    - Deduplicates dimension values in memory before writing.
    - Executes all INSERT statements and commits exactly once at the end.
    """
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )
    cur = conn.cursor()

    product_seen, store_seen, location_seen = set(), set(), set()
    customer_seen, date_seen, device_seen = set(), set(), set()

    try:
        for row in rows:
            # ---- PRODUCT ----
            if row.product_id and row.product_id not in product_seen:
                upsert_product_dimension(cur, (row.product_id,))
                product_seen.add(row.product_id)

            # ---- STORE ----
            if row.store_id and row.store_id not in store_seen:
                store_name = store_transformer(row.store_id)
                upsert_store_dimension(cur, (row.store_id, store_name))
                store_seen.add(row.store_id)

            # ---- LOCATION ----
            loc_id = getattr(row, "location_id", None)
            if loc_id and loc_id not in location_seen:
                loc_tuple = (
                    loc_id,
                    row.country_name,
                    row.country_short,
                    row.region_name,
                    row.city_name,
                )
                upsert_location_dimension(cur, loc_tuple)
                location_seen.add(loc_id)

            # ---- CUSTOMER ----
            customer_data = customer_transformer(
                customer_id=row.device_id,
                email_address=row.email_address,
                user_id_db=row.user_id_db,
            )
            cid = customer_data["customer_id"]
            if cid and cid not in customer_seen:
                upsert_customer_dimension(
                    cur,
                    (cid, customer_data["email_address"], customer_data["user_id_db"]),
                )
                customer_seen.add(cid)

            # ---- DEVICE (parse UA once for both browser and OS) ----
            if row.user_agent or row.resolution:
                device_data = device_transformer(row.user_agent, row.resolution)
                did = device_data["device_id"]
                if did not in device_seen:
                    # Fix #3: parse UA string a single time
                    browser_name, os_name = parse_user_agent(row.user_agent)
                    upsert_device_dimension(
                        cur,
                        (
                            did,
                            device_data["user_agent"],
                            device_data["resolution"],
                            os_name,
                            browser_name,
                        ),
                    )
                    device_seen.add(did)

            # ---- DATE ----
            if row.time_stamp:
                date_data = date_transformer(row.time_stamp)
                if date_data:
                    date_id = date_data["date_id"]
                    if date_id not in date_seen:
                        date_tuple = (
                            date_id,
                            date_data["full_date"],
                            date_data["date_of_week"],
                            date_data["date_of_week_short"],
                            date_data["is_weekday_or_weekend"],
                            date_data["day_of_month"],
                            date_data["day_of_year"],
                            date_data["week_of_year"],
                            date_data["quarter_number"],
                            date_data["year_number"],
                            date_data["year_month"],
                        )
                        upsert_date_dimension(cur, date_tuple)
                        date_seen.add(date_id)

        # Fix #4: single commit for the entire partition
        conn.commit()

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting dimensions in partition: {e}")
        raise
    finally:
        cur.close()
        conn.close()
