"""
Custom Data Archival Operators for Airflow.

Automates data retention policies and database maintenance.
"""

from airflow.models import BaseOperator
from airflow.utils.context import Context

from plugins.hooks.postgres_hook_ext import PostgresExtendedHook


class PostgresArchivalOperator(BaseOperator):
    """
    Operator that manages data archival and table optimization.

    :param table_name: Table to archive/optimize.
    :param retention_days: Records older than this will be archived.
    :param optimize: Whether to run VACUUM ANALYZE after archival.
    :param postgres_conn_id: Connection ID to use.
    """

    ui_color = "#fff0f5"

    def __init__(
        self,
        table_name: str,
        retention_days: int = 30,
        optimize: bool = True,
        postgres_conn_id: str = "postgres_streaming",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.retention_days = retention_days
        self.optimize = optimize
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Context):
        """Execute archival and maintenance."""
        self.log.info(f"Starting archival for table: {self.table_name}")
        hook = PostgresExtendedHook(postgres_conn_id=self.postgres_conn_id)

        # 1. Archival process
        try:
            # We call the method from your existing hook
            count = hook.archive_old_events(days_to_keep=self.retention_days)
            self.log.info(
                f"✅ Successfully archived {count} records from {self.table_name}"
            )
            context["ti"].xcom_push(key="archived_count", value=count)
        except Exception as e:
            self.log.error(f"💥 Failed to archive records: {e}")
            raise

        # 2. Optimization (Vacuum)
        if self.optimize:
            self.log.info(f"Running VACUUM ANALYZE on {self.table_name}...")
            try:
                hook.vacuum_table(self.table_name)
                self.log.info(f"✅ Optimization complete for {self.table_name}")
            except Exception as e:
                self.log.warning(
                    f"⚠️ Optimization failed for {self.table_name} (non-critical): {e}"
                )
