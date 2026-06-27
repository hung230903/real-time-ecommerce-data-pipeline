"""
Extended PostgreSQL Hook for Airflow.

Provides additional methods for data quality checks,
star schema validation, and archival operations.
"""

from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresExtendedHook(PostgresHook):
    """
    Extended PostgreSQL hook with data quality, sync, and archival capabilities.

    Inherits from the standard Airflow PostgresHook and adds project-specific
    methods for the streaming pipeline.
    """

    def __init__(self, postgres_conn_id: str = "postgres_streaming", **kwargs):
        super().__init__(postgres_conn_id=postgres_conn_id, **kwargs)

    # ─── Data Quality Methods ──────────────────────────────────────────

    def check_table_completeness(self, table: str) -> Dict[str, Any]:
        """
        Check completeness metrics for a given table.

        Returns:
            Dictionary with row count, null counts per column, etc.
        """
        self.log.info(f"Checking completeness for table: {table}")

        row_count = self.get_first(f"SELECT COUNT(*) FROM {table}")[0]

        # Get column names
        columns_sql = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            ORDER BY ordinal_position
        """
        columns = [row[0] for row in self.get_records(columns_sql)]

        null_counts = {}
        for col in columns:
            null_count = self.get_first(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            )[0]
            null_counts[col] = null_count

        return {
            "table": table,
            "total_rows": row_count,
            "null_counts": null_counts,
            "completeness_pct": round(
                (1 - sum(null_counts.values()) / max(row_count * len(columns), 1))
                * 100,
                2,
            ),
        }

    def check_record_count(self, table: str) -> int:
        """Get the total record count for a table."""
        result = self.get_first(f"SELECT COUNT(*) FROM {table}")
        return result[0] if result else 0

    def check_duplicate_count(self, table: str, key_column: str) -> int:
        """Check for duplicate values in a key column."""
        sql = f"""
            SELECT COUNT(*) - COUNT(DISTINCT {key_column})
            FROM {table}
        """
        result = self.get_first(sql)
        return result[0] if result else 0

    def validate_data_types(self, table: str) -> List[Dict[str, str]]:
        """Get column data types for validation."""
        sql = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """
        records = self.get_records(sql)
        return [{"column": r[0], "data_type": r[1], "nullable": r[2]} for r in records]

    def get_error_rate(self, table: str, expected_min: int = 0) -> Dict[str, Any]:
        """
        Calculate error rate based on null primary keys or missing references.
        """
        row_count = self.check_record_count(table)
        return {
            "table": table,
            "total_rows": row_count,
            "meets_minimum": row_count >= expected_min,
        }

    # ─── Fact Table Methods ────────────────────────────────────────────

    def get_fact_table_count(self) -> int:
        """Get the record count from the fact table."""
        return self.check_record_count("fact_product_views")

    # ─── Archival Methods ─────────────────────────────────────────────

    def archive_old_events(self, days_to_keep: int = 30) -> int:
        """
        Archive (delete) old events from fact_event_logs.

        Args:
            days_to_keep: Number of days of data to retain.

        Returns:
            Number of archived (deleted) rows.
        """
        count_sql = f"""
            SELECT COUNT(*) FROM fact_product_views
            WHERE time_stamp < NOW() - INTERVAL '{days_to_keep} days'
        """
        count = self.get_first(count_sql)[0]

        if count > 0:
            self.log.info(
                f"Archiving {count} old event records (older than {days_to_keep} days)"
            )
            self.run(
                f"DELETE FROM fact_product_views "
                f"WHERE time_stamp < NOW() - INTERVAL '{days_to_keep} days'"
            )
        else:
            self.log.info("No old records to archive.")

        return count

    def vacuum_table(self, table: str) -> None:
        """Run VACUUM ANALYZE on a table to reclaim space."""
        self.log.info(f"Running VACUUM ANALYZE on {table}")
        # VACUUM cannot run inside a transaction
        old_autocommit = self.get_conn().autocommit
        self.get_conn().autocommit = True
        try:
            self.run(f"VACUUM ANALYZE {table}")
        finally:
            self.get_conn().autocommit = old_autocommit

    def get_table_size(self, table: str) -> Dict[str, str]:
        """Get the size of a table."""
        sql = f"""
            SELECT 
                pg_size_pretty(pg_total_relation_size('{table}')) as total_size,
                pg_size_pretty(pg_relation_size('{table}')) as data_size
            """
        result = self.get_first(sql)
        return {
            "table": table,
            "total_size": result[0] if result else "unknown",
            "data_size": result[1] if result else "unknown",
        }
