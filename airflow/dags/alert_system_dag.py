"""
Alert System DAG.

Centralized alerting framework with configurable alert rules,
severity levels, escalation, and multi-channel notification.

Schedule: Every 10 minutes.
"""

from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ─── Alert Configuration ────────────────────────────────────────────

ALERT_RULES = [
    {
        "name": "kafka_broker_down",
        "description": "Kafka broker is not reachable",
        "severity": "CRITICAL",
        "check_type": "kafka_health",
    },
    {
        "name": "fact_table_empty",
        "description": "Fact table has no data",
        "severity": "HIGH",
        "check_type": "table_count",
        "table": "fact_product_views",
        "threshold": 0,
    },
    {
        "name": "low_data_quality",
        "description": "Data quality check found issues",
        "severity": "MEDIUM",
        "check_type": "data_quality",
    },
    {
        "name": "high_storage_usage",
        "description": "Database storage is growing unusually",
        "severity": "LOW",
        "check_type": "storage_check",
    },
    {
        "name": "dag_failure_detected",
        "description": "One or more DAGs failed recently",
        "severity": "CRITICAL",
        "check_type": "dag_failure",
    },
    {
        "name": "data_stagnation",
        "description": "Spark streaming data freshness check",
        "severity": "HIGH",
        "check_type": "data_freshness",
        "table": "fact_product_views",
        "timestamp_column": "time_stamp",
        "max_delay_minutes": 15,
    },
]

SEVERITY_PRIORITY = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}


# ─── Task Callables ──────────────────────────────────────────────────


def _evaluate_alert_rules(ti: TaskInstance, **kwargs):
    """Evaluate all alert rules and identify triggered alerts."""
    triggered_alerts = []

    for rule in ALERT_RULES:
        try:
            result = _check_rule(rule)

            # Handle both boolean and tuple (boolean, dynamic_description) returns
            if isinstance(result, tuple):
                is_triggered, dynamic_desc = result
            else:
                is_triggered = result
                dynamic_desc = rule["description"]

            if is_triggered:
                alert = {
                    "name": rule["name"],
                    "description": dynamic_desc,
                    "severity": rule["severity"],
                    "priority": SEVERITY_PRIORITY.get(rule["severity"], 5),
                    "triggered_at": datetime.now().isoformat(),
                }
                triggered_alerts.append(alert)
                print(f"🚨 ALERT: {rule['severity']} - {rule['name']}: {dynamic_desc}")
            else:
                print(f"✅ Rule OK: {rule['name']}")
        except Exception as e:
            print(f"⚠️  Error evaluating rule {rule['name']}: {e}")

    # Sort by priority (lower = higher severity)
    triggered_alerts.sort(key=lambda x: x["priority"])

    ti.xcom_push(key="triggered_alerts", value=triggered_alerts)
    ti.xcom_push(key="alert_count", value=len(triggered_alerts))

    return triggered_alerts


def _check_rule(rule: Dict) -> bool:
    """Check a single alert rule. Returns True if alert should trigger."""
    check_type = rule.get("check_type")

    if check_type == "kafka_health":
        try:
            import socket
            from contextlib import closing

            from airflow.models import Variable

            from plugins.hooks.kafka_hook import KafkaMonitoringHook

            # 1. Check Local Kafka
            local_hook = KafkaMonitoringHook(kafka_conn_id="kafka_default")
            local_up = local_hook.check_broker_connectivity(timeout=5)

            # 2. Check Remote Kafka (via Variable with Failover)
            remote_servers = Variable.get("SERVER_BOOTSTRAP_SERVERS", default_var=None)
            remote_up = True
            if remote_servers:
                import re

                remote_servers = re.sub(
                    r"^Val\s*\(Value\):\s*", "", remote_servers, flags=re.IGNORECASE
                ).strip()
                remote_up = False  # Assume down until one succeeds
                for server in remote_servers.split(","):
                    try:
                        server = server.strip()
                        if not server:
                            continue
                        host_port = server.split(":")
                        host = host_port[0].strip()
                        port = int(host_port[1].strip()) if len(host_port) > 1 else 9092
                        with closing(
                            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        ) as sock:
                            sock.settimeout(5)
                            if sock.connect_ex((host, port)) == 0:
                                remote_up = True
                                break
                    except:
                        continue

            return not (local_up and remote_up)
        except Exception:
            return True  # If we can't check, assume unhealthy

    elif check_type == "table_count":
        try:
            from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

            hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
            count = hook.check_record_count(rule.get("table", "fact_product_views"))
            return count <= rule.get("threshold", 0)
        except Exception:
            return False

    elif check_type == "data_quality":
        try:
            from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

            hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
            dup_count = hook.check_duplicate_count("fact_product_views", "fact_id")
            return dup_count > 0
        except Exception:
            return False

    elif check_type == "data_freshness":
        try:
            from datetime import datetime, timedelta

            from plugins.hooks.postgres_hook_ext import PostgresExtendedHook

            hook = PostgresExtendedHook(postgres_conn_id="postgres_streaming")
            table = rule.get("table", "fact_product_views")
            ts_col = rule.get("timestamp_column", "time_stamp")
            max_delay = rule.get("max_delay_minutes", 15)

            sql = f"SELECT MAX({ts_col}) FROM {table}"
            record = hook.get_first(sql)

            if not record or not record[0]:
                return False  # Table is empty, fact_table_empty rule will handle this

            latest_ts = record[0]

            if type(latest_ts) is str:
                # Fallback if DB driver returns string
                try:
                    latest_ts = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                except ValueError:
                    latest_ts = datetime.strptime(
                        latest_ts.split(".")[0], "%Y-%m-%d %H:%M:%S"
                    )

            now = datetime.utcnow()
            if latest_ts.tzinfo:
                now = datetime.now(latest_ts.tzinfo)

            if now - latest_ts > timedelta(minutes=max_delay):
                desc = f"Streaming stuck: No new data in {table} since {latest_ts.strftime('%H:%M:%S')} (>{max_delay} mins delay)"
                return True, desc

            return False
        except Exception as e:
            print(f"Error checking data freshness: {e}")
            return False

    elif check_type == "storage_check":
        # Placeholder for storage monitoring
        return False

    elif check_type == "dag_failure":
        try:
            from datetime import timedelta

            from airflow.models import DagRun
            from airflow.utils import timezone
            from airflow.utils.session import create_session
            from airflow.utils.state import State

            with create_session() as session:
                # Check for DAG runs that failed in the last 15 minutes
                time_threshold = timezone.utcnow() - timedelta(minutes=15)
                failed_runs = (
                    session.query(DagRun)
                    .filter(
                        DagRun.state == State.FAILED, DagRun.end_date >= time_threshold
                    )
                    .all()
                )

                if failed_runs:
                    failed_dag_ids = list(set([run.dag_id for run in failed_runs]))
                    desc = f"Failed DAGs detected in the last 15 mins: {', '.join(failed_dag_ids)}"
                    return True, desc
                return False
        except Exception as e:
            print(f"Error checking dag failures: {e}")
            return False

    return False


def _classify_and_prioritize(ti: TaskInstance, **kwargs):
    """Classify alerts by severity and apply escalation rules."""
    alerts = ti.xcom_pull(task_ids="evaluate_alert_rules", key="triggered_alerts")

    if not alerts:
        print("ℹ️  No alerts triggered.")
        ti.xcom_push(
            key="classified_alerts", value={"alerts": [], "requires_escalation": False}
        )
        return

    classified = {
        "CRITICAL": [],
        "HIGH": [],
        "MEDIUM": [],
        "LOW": [],
    }

    for alert in alerts:
        severity = alert.get("severity", "LOW")
        classified[severity].append(alert)

    requires_escalation = len(classified["CRITICAL"]) > 0

    result = {
        "alerts": alerts,
        "classified": classified,
        "requires_escalation": requires_escalation,
        "summary": {
            "total": len(alerts),
            "critical": len(classified["CRITICAL"]),
            "high": len(classified["HIGH"]),
            "medium": len(classified["MEDIUM"]),
            "low": len(classified["LOW"]),
        },
    }

    ti.xcom_push(key="classified_alerts", value=result)
    print(f"📊 Alert Summary: {result['summary']}")
    return result


def _send_notifications(ti: TaskInstance, **kwargs):
    """Send notifications for triggered alerts via Telegram."""
    import requests
    from airflow.models import Variable

    classified = ti.xcom_pull(
        task_ids="classify_and_prioritize", key="classified_alerts"
    )

    if not classified or not classified.get("alerts"):
        print("ℹ️  No notifications to send.")
        return

    # Print header to logs
    print("=" * 60)
    print("📬 ALERT NOTIFICATIONS")
    print("=" * 60)

    # Prepare message for Telegram
    message_lines = ["🚨 <b>DATA PIPELINE ALERTS</b> 🚨\n"]

    for alert in classified["alerts"]:
        severity_emoji = {
            "CRITICAL": "🔴",
            "HIGH": "🟠",
            "MEDIUM": "🟡",
            "LOW": "🟢",
        }.get(alert["severity"], "⚪")

        # 1. Print to logs
        print(f"  {severity_emoji} [{alert['severity']}] {alert['name']}")
        print(f"     {alert['description']}")
        print(f"     Triggered: {alert['triggered_at']}")
        print()

        # 2. Add to Telegram message
        message_lines.append(
            f"{severity_emoji} <b>[{alert['severity']}] {alert['name']}</b>"
        )
        message_lines.append(f"<i>{alert['description']}</i>")
        message_lines.append(f"⏱ Triggered: {alert['triggered_at']}\n")

    if classified.get("requires_escalation"):
        print("⚠️  ESCALATION REQUIRED: Critical alerts detected!")
        message_lines.append("⚠️ <b>ESCALATION REQUIRED:</b> Critical alerts detected!")

    print("=" * 60)
    message_text = "\n".join(message_lines)

    # Fetch credentials from Airflow Variables (with strip to remove invisible characters)
    bot_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var="").strip()
    chat_id = Variable.get("TELEGRAM_CHAT_ID", default_var="").strip()

    if bot_token and chat_id:
        try:
            telegram_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": message_text, "parse_mode": "HTML"}
            response = requests.post(telegram_url, json=payload, timeout=10)
            response.raise_for_status()
            print("✅ Telegram notification sent successfully!")
        except Exception as e:
            print(f"❌ Failed to send Telegram notification: {e}")
    else:
        print("ℹ️  Telegram credentials not configured. Skipping Telegram notification.")
        print(
            "   Please set 'TELEGRAM_BOT_TOKEN' and 'TELEGRAM_CHAT_ID' in Airflow Variables."
        )

    notifications_sent = {
        "total_alerts": len(classified["alerts"]),
        "escalated": classified.get("requires_escalation", False),
        "channels": ["log", "telegram"] if (bot_token and chat_id) else ["log"],
        "timestamp": datetime.now().isoformat(),
    }

    ti.xcom_push(key="notifications_sent", value=notifications_sent)
    return notifications_sent


# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="alert_system",
    default_args=default_args,
    description="Centralized alert system with rules, severity, and escalation",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["alerting", "monitoring"],
    doc_md="""
    ## Alert System DAG
    
    Runs every 10 minutes to:
    1. Evaluate all alert rules
    2. Classify alerts by severity
    3. Apply escalation policies
    4. Send notifications
    
    ### Alert Severities
    - **CRITICAL**: Immediate attention required (e.g., broker down)
    - **HIGH**: Significant issue (e.g., data stuck)
    - **MEDIUM**: Non-urgent issue (e.g., quality degradation)
    - **LOW**: Informational (e.g., storage growth)
    """,
) as dag:
    evaluate_rules = PythonOperator(
        task_id="evaluate_alert_rules",
        python_callable=_evaluate_alert_rules,
    )

    classify = PythonOperator(
        task_id="classify_and_prioritize",
        python_callable=_classify_and_prioritize,
    )

    notify = PythonOperator(
        task_id="send_notifications",
        python_callable=_send_notifications,
    )

    evaluate_rules >> classify >> notify
