"""
Custom Data Quality Operators for Airflow.

Automates SQL-based data validation checks.
"""

from typing import Any, Dict, List

from airflow.models import BaseOperator
from airflow.utils.context import Context

from plugins.hooks.postgres_hook_ext import PostgresExtendedHook


class DataQualityOperator(BaseOperator):
    """
    Operator that runs SQL checks against a PostgreSQL database.

    :param sql_checks: List of dicts with 'sql' and 'expected_result' keys.
    :param postgres_conn_id: Connection ID to use.
    """

    ui_color = "#f0f8ff"

    def __init__(
        self,
        sql_checks: List[Dict[str, Any]],
        postgres_conn_id: str = "postgres_streaming",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_checks = sql_checks
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Context):
        """Execute the SQL checks."""
        self.log.info(f"Starting Data Quality checks on: {self.postgres_conn_id}")
        hook = PostgresExtendedHook(postgres_conn_id=self.postgres_conn_id)

        failed_checks = []

        for check in self.sql_checks:
            sql = check.get("sql")
            expected = check.get("expected_result")
            check_name = check.get("name", sql)

            self.log.info(f"Running check: {check_name}")

            try:
                records = hook.get_first(sql)

                if not records or records[0] != expected:
                    failed_checks.append(
                        f"Check failed: {check_name}. Expected {expected}, got {records[0] if records else 'None'}"
                    )
                    self.log.error(f"❌ Check FAILED: {check_name}")
                else:
                    self.log.info(f"✅ Check PASSED: {check_name}")

            except Exception as e:
                failed_checks.append(f"Check error: {check_name}. Error: {e}")
                self.log.error(f"💥 SYSTEM ERROR during check: {check_name}")

        if failed_checks:
            self.log.error(
                f"Data Quality checks completed with {len(failed_checks)} failure(s)."
            )
            raise ValueError(
                "Data Quality validation failed:\n" + "\n".join(failed_checks)
            )

        self.log.info("🏆 All Data Quality checks PASSED successfully!")
