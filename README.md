# Real-time Streaming Data Pipeline

An end-to-end real-time data pipeline: **Kafka → Spark Structured Streaming → PostgreSQL Star Schema**, with an
analytics dashboard built on Streamlit.

## Table of Contents

- [Project Flow Overview](#project-flow-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
  - [1. Kafka Producer](#1-kafka-producer-srckafkaproducerpy)
  - [2. Spark Structured Streaming](#2-spark-structured-streaming-srcstreaming)
  - [3. Data Processing](#3-data-processing-srcprocessing)
  - [4. Database Load](#4-database-load-srcloaddb_upserterpy)
- [Star Schema](#star-schema)
  - [Dimension Tables](#dimension-tables)
  - [Fact Table](#fact-table)
- [Kafka Cluster](#kafka-cluster)
- [Streamlit Dashboard](#streamlit-dashboard)
- [Testing](#testing)
- [Setup](#setup)
  - [1. Create Docker network](#1-create-docker-network)
  - [2. Start Kafka cluster](#2-start-kafka-cluster)
  - [3. Start PostgreSQL](#3-start-postgresql)
  - [4. Create the Star Schema database](#4-create-the-star-schema-database)
  - [5. Build the Spark image](#5-build-the-spark-image)
  - [6. Configure environment variables](#6-configure-environment-variables)
  - [7. Run the Kafka Producer](#7-run-the-kafka-producer)
  - [8. Run Spark Streaming](#8-run-spark-streaming)
- [Environment Variables](#environment-variables)
- [Monitoring](#monitoring)
- [References](#references)

## Project Flow Overview

<img src="images/diagrams/streaming.svg"/>

## Tech Stack

| Component    | Technology                                                           |
| ------------ | -------------------------------------------------------------------- |
| Messaging    | Apache Kafka 7.6.1 (Confluent, KRaft mode, 3 brokers)                |
| Processing   | Apache Spark 3.5.x (Structured Streaming, PySpark)                   |
| Database     | PostgreSQL 16.3 (Star Schema — Kimball)                              |
| Orchestrator | Apache Airflow 2.10.4 (see [`airflow/README.md`](airflow/README.md)) |
| Dashboard    | Streamlit + Plotly                                                   |
| IP Lookup    | IP2Location (offline BIN database)                                   |
| Cluster      | Hadoop YARN + Docker                                                 |
| Monitoring   | AKHQ (Kafka) · Adminer (PostgreSQL)                                  |

## Project Structure

```
.
├── src/                            # Core source code
│   ├── kafka/
│   │   └── producer.py             # Kafka producer: forwards data from Server → Local
│   ├── streaming/
│   │   ├── spark_runner.py          # Initializes SparkSession, reads from Kafka, runs streaming
│   │   └── spark_streaming.py       # Batch processing: upsert dimensions → enrich → write fact
│   ├── processing/
│   │   ├── data_transformer.py      # Data transformation for dimensions (date, customer, device, store)
│   │   └── ip_enricher.py           # IP → Location enrichment using IP2Location
│   ├── load/
│   │   └── db_upserter.py           # Upserts data into dimension & fact tables (PostgreSQL)
│   ├── schema/
│   │   └── schemas.py               # PySpark StructType schemas (EVENT_SCHEMA, LOCATION_SCHEMA)
│   ├── report_dashboard/
│   │   └── dashboard.py             # Streamlit real-time analytics dashboard
│   └── setup_db_schema.sql          # DDL to create database & Star Schema tables
│
├── tests/                           # Unit tests for source code
│   ├── kafka/                       # Kafka producer tests
│   ├── load/                        # DB upsert tests
│   └── processing/                  # Data transformer & enricher tests
│
├── config/
│   ├── base.py                      # Centralized configuration (Kafka, Postgres, Spark) from .env
│   └── logger.py                    # Logging configuration with RotatingFileHandler
│
├── build/                           # Docker build & infrastructure
│   ├── kafka/                       # 3-node Kafka cluster (KRaft) + AKHQ monitor
│   ├── spark/                       # Custom Spark image (Miniconda + dependencies)
│   ├── postgres/                    # PostgreSQL 16.3 + Adminer
│   └── hadoop/                      # Hadoop YARN cluster
│
├── hadoop-conf/                     # Hadoop config files (core-site, yarn-site, ...)
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   └── yarn-site.xml
│
├── airflow/                         # Airflow orchestration (see airflow/README.md)
│
├── main.py                          # Entry point — runs Spark Structured Streaming (deprecated)
├── environment.yml                  # Conda environment for PySpark workers
├── pyproject.toml                   # Project metadata & Python dependencies
├── .env.example                     # Environment variables template
└── IP2LOCATION-LITE-DB11.BIN        # IP2Location database (offline IP → Geo lookup)
```

## Data Flow

### 1. Kafka Producer (`src/kafka/producer.py`)

Consumes data from a **remote Kafka Server** and forwards it to a **local Kafka cluster**.

- Uses `confluent-kafka` Consumer (remote) + Producer (local)
- Manual offset commit every 100 messages to prevent data loss
- SASL_PLAINTEXT authentication for both Kafka clusters
- Graceful shutdown with flush before closing

```
 Kafka Server (remote)                   Local Kafka Cluster
┌─────────────────────┐                ┌─────────────────────┐
│ topic: product_view │ ───consumer──► │ topic: product_view │
│ SASL_PLAINTEXT      │ ───produce───► │ 3 brokers (KRaft)   │
│ 2 brokers           │                │ SASL_PLAINTEXT      │
└─────────────────────┘                └─────────────────────┘
```

### 2. Spark Structured Streaming (`src/streaming/`)

Reads real-time data from the local Kafka cluster, processing in micro-batches every 15 seconds.

**`spark_runner.py`** — Pipeline initialization:

- Creates a `SparkSession` with YARN / client deploy-mode
- Reads from Kafka topic using Spark Structured Streaming
- Parses JSON → enriches with IP location → calls `process_batch()`

**`spark_streaming.py`** — Core processing logic:

- **Upsert dimensions**: product, store, location, customer, device, date
- **Enrich batch**: joins the batch DataFrame with dimension keys, generates `fact_id` (SHA-256)
- **Write fact**: upserts into `fact_product_views` using `execute_batch()` (psycopg2)

### 3. Data Processing (`src/processing/`)

| Module                | Description                                                    |
| --------------------- | -------------------------------------------------------------- |
| `data_transformer.py` | Transforms store_name, customer_id, device_id (SHA-256), date  |
| `ip_enricher.py`      | Looks up IP → country, region, city via IP2Location offline DB |

### 4. Database Load (`src/load/db_upserter.py`)

Performs upsert operations (INSERT ... ON CONFLICT) for all dimension tables and returns the generated Surrogate Keys (`SERIAL`) for Fact table enrichment:

| Function                      | Target Table   | Conflict Key  |
| ----------------------------- | -------------- | ------------- |
| `upsert_product_dimension()`  | `dim_product`  | `product_id`  |
| `upsert_store_dimension()`    | `dim_store`    | `store_id`    |
| `upsert_location_dimension()` | `dim_location` | `location_id` |
| `upsert_customer_dimension()` | `dim_customer` | `customer_id` |
| `upsert_device_dimension()`   | `dim_device`   | `device_id`   |
| `upsert_date_dimension()`     | `dim_date`     | `date_id`     |

## Star Schema

<img src="./images/database/spark_streaming_schema.png">

### Dimension Tables

| Table          | Primary Key (Surrogate) | Natural Key (Unique) & Description                      |
| -------------- | ----------------------- | ------------------------------------------------------- |
| `dim_product`  | `product_key` (SERIAL)  | `product_id` — Product information (id, name, sku...)   |
| `dim_store`    | `store_key` (SERIAL)    | `store_id` — Store details (store_id, store_name)       |
| `dim_location` | `location_key` (SERIAL) | `location_id` — Geographic location (country, city...)  |
| `dim_customer` | `customer_key` (SERIAL) | `customer_id` — Customer identity (email, user_id...)   |
| `dim_device`   | `device_key` (SERIAL)   | `device_id` — Device info (user_agent, resolution...)   |
| `dim_date`     | `date_id` (INTEGER)     | `date_id` — Date attributes (full_date, day_of_week...) |

### Fact Table

| Table                | Primary Key | Foreign Keys                                                                        |
| -------------------- | ----------- | ----------------------------------------------------------------------------------- |
| `fact_product_views` | `fact_id`   | `product_key`, `store_key`, `location_key`, `customer_key`, `device_key`, `date_id` |

Additional columns: `ip_address`, `time_stamp`, `collection`, `current_url`, `referrer_url`.

## Kafka Cluster

The local Kafka cluster consists of **3 brokers** running in **KRaft mode** (no Zookeeper required):

| Container | Internal Port | External Port | Node ID |
| --------- | ------------- | ------------- | ------- |
| `kafka-0` | 9092          | 9094          | 0       |
| `kafka-1` | 9092          | 9194          | 1       |
| `kafka-2` | 9092          | 9294          | 2       |

- Image: `confluentinc/cp-kafka:7.6.1`
- Authentication: SASL_PLAINTEXT
- Default topic: `product_view` (3 partitions, replication factor 3)
- Monitoring: **AKHQ** at `http://localhost:8180`

## Streamlit Dashboard

A real-time analytics dashboard with 6 sections:

1. **Top 10 Product IDs** — most viewed products
2. **Top 10 Countries** — countries with the highest view counts
3. **Top 5 Referrer URLs** — primary traffic sources
4. **Store Views by Country** — view count per store for a selected country
5. **Hourly Views per Product** — hourly view distribution for a selected product
6. **Browser & OS Distribution** — device/browser breakdown

```bash
uv run python -m streamlit run src/report_dashboard/dashboard.py
```

## Testing

The project includes a robust unit testing suite using `pytest` to validate core logic without connecting to external services. The tests are located in the `tests/` directory:

- **Processing (`tests/processing/`)**: Validates data transformations (date, user-agent parsing) and `IP2Location` geography enrichment using `unittest.mock`.
- **Database Load (`tests/load/`)**: Mocks PostgreSQL cursors to verify dimension `UPSERT` statements are structured correctly and capture surrogate keys.
- **Kafka Producer (`tests/kafka/`)**: Asserts message delivery callbacks, exception handling, and correct initialization behaviors.

### Running the Tests

Ensure your virtual environment is configured with development dependencies:

```bash
# Sync and install all core + dev dependencies
uv sync

# Run the full test suite
uv run pytest tests/

# Run tests with verbose output
uv run pytest tests/ -v
```

## Setup

### 1. Create Docker network

```bash
docker network create streaming-network --driver bridge
```

### 2. Start Kafka cluster

```bash
cd build/kafka
docker compose up -d
```

### 3. Start PostgreSQL

```bash
cd build/postgres
docker compose up -d
```

### 4. Create the Star Schema database

```bash
psql -h localhost -p 5432 -U postgres -f src/setup_db_schema.sql
```

### 5. Build the Spark image

```bash
cd build/spark
docker build -t unigap/spark:3.5 .
docker volume create spark_data
docker volume create spark_lib
```

### 6. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 7. Run the Kafka Producer

```bash
uv run -m src.kafka.producer
```

### 8. Run Spark Streaming

**Run on YARN (via Docker):**

```bash

docker container stop kafka-streaming || true && \
docker container rm kafka-streaming || true && \
docker run -ti --name kafka-streaming \
  --env-file .env \
  --network=streaming-network \
  -v $(pwd):/spark \
  -v spark_lib:/home/spark/.ivy2 \
  -v spark_data:/data \
  -e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
  -e PYSPARK_DRIVER_PYTHON='/home/spark/miniconda3/envs/pyspark_conda_env/bin/python' \
  -e PYSPARK_PYTHON='./environment/bin/python' \
  unigap/spark:3.5 bash -c "
    source ~/miniconda3/bin/activate && \
    (conda env update --file /spark/environment.yml --prune || conda env create --file /spark/environment.yml) && \
    conda activate pyspark_conda_env && \
    cd /spark && \
    export PYTHONPATH=\$PYTHONPATH:/spark && \
    conda pack -f -o /tmp/pyspark_conda_env.tar.gz && \
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --conf spark.yarn.dist.archives=/tmp/pyspark_conda_env.tar.gz#environment \
      --deploy-mode client \
      --master yarn \
      src/streaming/spark_runner.py"

```

## Environment Variables

| Variable                   | Description                      | Example                                  |
| -------------------------- | -------------------------------- | ---------------------------------------- |
| `SERVER_BOOTSTRAP_SERVERS` | Remote Kafka brokers             | `host1:9094,host2:9094`                  |
| `SERVER_TOPIC`             | Topic on remote Kafka            | `product_view`                           |
| `SERVER_SASL_USERNAME`     | Remote SASL username             |                                          |
| `SERVER_SASL_PASSWORD`     | Remote SASL password             |                                          |
| `LOCAL_BOOTSTRAP_SERVERS`  | Local Kafka brokers              | `localhost:9094,localhost:9194`          |
| `KAFKA_BOOTSTRAP_SERVERS`  | Kafka brokers for Spark (Docker) | `kafka-0:9092,kafka-1:9092,kafka-2:9092` |
| `KAFKA_USERNAME`           | Local SASL username              |                                          |
| `KAFKA_PASSWORD`           | Local SASL password              |                                          |
| `POSTGRES_HOST`            | PostgreSQL host                  | `localhost`                              |
| `POSTGRES_PORT`            | PostgreSQL port                  | `5432`                                   |
| `POSTGRES_DB`              | Database name                    | `spark_streaming_project`                |
| `POSTGRES_USER`            | PostgreSQL username              | `postgres`                               |
| `POSTGRES_PASSWORD`        | PostgreSQL password              |                                          |
| `CHECKPOINT_PATH`          | Spark checkpoint directory       | `file:///tmp/spark_checkpoints`          |
| `IP2LOCATION_DB_PATH`      | Path to IP2Location DB file      | `/spark/IP2LOCATION-LITE-DB11.BIN`       |

## Monitoring

| Service      | URL                    | Description                                                 |
| ------------ | ---------------------- | ----------------------------------------------------------- |
| AKHQ         | http://localhost:8180  | Kafka cluster monitor                                       |
| Adminer      | http://localhost:8380  | PostgreSQL admin UI                                         |
| Airflow      | http://localhost:18080 | DAG orchestration UI                                        |
| Telegram Bot | —                      | Real-time push alerts (Kafka, DAG failures, data freshness) |

## References

- [Confluent Kafka Docker](https://docs.confluent.io/platform/current/installation/docker/image-reference.html)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [IP2Location Python](https://www.ip2location.com/development-libraries/ip2location/python)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
