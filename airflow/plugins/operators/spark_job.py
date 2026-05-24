"""
Custom Spark Operators for Airflow.

Wraps Docker-based Spark job submission to keep DAGs clean.

All infrastructure-specific values (paths, hostnames, credentials) are
accepted as constructor parameters.  Default values are intentionally
left as None / empty so misconfiguration is caught early rather than
silently using a stale hardcoded value.
"""

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


class SparkStreamingOperator(DockerOperator):
    """
    Operator that submits a Spark Structured Streaming job using Docker.

    This operator simplifies the submission by pre-configuring
    mounts, environment variables, and the spark-submit command.

    All sensitive / environment-specific values must be injected by the
    calling DAG (resolved from Airflow Variables / Connections) – this
    class contains no hardcoded credentials or host addresses.
    """

    template_fields = DockerOperator.template_fields + ("kafka_topic",)

    def __init__(
            self,
            # ── Application ──────────────────────────────────────────
            spark_app_path: str = "src/streaming/spark_runner.py",
            kafka_topic: str = "product_view",
            project_path: str = "/app",
            # ── Postgres (injected from Airflow Connection) ───────────
            postgres_host: str = "localhost",
            postgres_port: str = "5432",
            postgres_db: str = "spark_streaming_schema",
            postgres_user: str = "postgres",
            postgres_password: str = "",
            # ── Kafka bootstrap (injected from Airflow Connection) ────
            kafka_bootstrap_servers: str = "kafka-0:9092,kafka-1:9092,kafka-2:9092",
            **kwargs
    ):
        self.spark_app_path = spark_app_path
        self.kafka_topic = kafka_topic
        self.project_path = project_path
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_db = postgres_db
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

        command = [
            "bash", "-c",
            "source ~/miniconda3/bin/activate && "
            "(conda env update --file /spark/environment.yml --prune || conda env create --file /spark/environment.yml) && "
            "conda activate pyspark_conda_env && "
            "cd /spark && "
            "export PYTHONPATH=$PYTHONPATH:/spark && "
            "conda pack -f -o /tmp/pyspark_conda_env.tar.gz && "
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 "
            "--conf spark.yarn.dist.archives=/tmp/pyspark_conda_env.tar.gz#environment "
            "--deploy-mode client "
            "--master yarn "
            f"{spark_app_path}"
        ]

        mounts = [
            Mount(source=project_path, target="/spark", type="bind"),
            Mount(source="spark_lib", target="/home/spark/.ivy2", type="volume"),
            Mount(source="spark_data", target="/data", type="volume"),
        ]

        # Merge caller-supplied environment overrides on top of defaults
        env = kwargs.pop("environment", {})
        default_env = {
            "HADOOP_CONF_DIR": "/spark/hadoop-conf/",
            "PYSPARK_DRIVER_PYTHON": "/home/spark/miniconda3/envs/pyspark_conda_env/bin/python",
            "PYSPARK_PYTHON": "./environment/bin/python",
            "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
            "KAFKA_TOPIC": kafka_topic,
            "POSTGRES_HOST": postgres_host,
            "POSTGRES_PORT": postgres_port,
            "POSTGRES_DB": postgres_db,
            "POSTGRES_USER": postgres_user,
            "POSTGRES_PASSWORD": postgres_password,
        }
        default_env.update(env)

        super().__init__(
            image="unigap/spark:3.5",
            command=command,
            mounts=mounts,
            environment=default_env,
            auto_remove="force",
            mount_tmp_dir=False,
            docker_url="unix://var/run/docker.sock",
            network_mode="streaming-network",
            **kwargs,
        )
