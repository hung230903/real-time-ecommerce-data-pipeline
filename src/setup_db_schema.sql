DROP DATABASE IF EXISTS spark_streaming_schema;
CREATE DATABASE spark_streaming_schema;

\c spark_streaming_schema;

CREATE TABLE dim_product
(
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(255) UNIQUE,
    suffix TEXT,
    product_name TEXT,
    sku TEXT,
    attribute_set_id INTEGER,
    type_id TEXT,
    min_price NUMERIC,
    max_price NUMERIC,
    collection_id TEXT,
    product_type_value TEXT,
    product_subtype_id INTEGER,
    store_code TEXT,
    gender TEXT
);


CREATE TABLE dim_store
(
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(255) UNIQUE,
    store_name VARCHAR(255)
);

CREATE TABLE dim_location
(
    location_key SERIAL PRIMARY KEY,
    location_id VARCHAR(255) UNIQUE,
    country_name VARCHAR(255),
    country_short VARCHAR(100),
    region_name VARCHAR(255),
    city_name VARCHAR(255)
);


CREATE TABLE dim_device
(
    device_key SERIAL PRIMARY KEY,
    device_id VARCHAR(255) UNIQUE,
    user_agent TEXT,
    resolution VARCHAR(50),
    os VARCHAR(100),
    browser VARCHAR(100)
);

CREATE TABLE dim_customer
(
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(255) UNIQUE,
    email_address VARCHAR(255),
    user_id_db VARCHAR(255)
);

CREATE TABLE dim_date
(
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    date_of_week VARCHAR(20),
    date_of_week_short VARCHAR(10),
    is_weekday_or_weekend VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    quarter_number INTEGER,
    year_number INTEGER,
    year_month VARCHAR(10)
);


CREATE TABLE fact_product_views
(
    fact_id VARCHAR(255) PRIMARY KEY,
    product_key INTEGER,
    store_key INTEGER,
    location_key INTEGER,
    customer_key INTEGER,
    device_key INTEGER,
    date_id INTEGER,
    ip_address VARCHAR(50),
    time_stamp TIMESTAMP,
    collection TEXT,
    current_url TEXT,
    referrer_url TEXT,
    CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES dim_product (product_key),
    CONSTRAINT fk_store FOREIGN KEY (store_key) REFERENCES dim_store (store_key),
    CONSTRAINT fk_location FOREIGN KEY (location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_device FOREIGN KEY (device_key) REFERENCES dim_device (device_key),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
);
