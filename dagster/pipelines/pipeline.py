import psycopg2
from dagster import (
    asset, job, op, schedule,
    Definitions, ScheduleDefinition,
    DefaultScheduleStatus, OpExecutionContext
)

# Database connection helper
DB_CONFIG = {
    "host":     "postgres-dw",
    "port":     5432,
    "dbname":   "pulse_dw",
    "user":     "pulse",
    "password": "pulse",
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# Op 1: Merchant daily reconciliation
# Reads all transactions for today and aggregates by merchant
@op
def run_merchant_reconciliation(context: OpExecutionContext):
    context.log.info("Starting merchant daily reconciliation")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO merchant_daily_summary (
                summary_date,
                merchant_id,
                service_type,
                total_transactions,
                total_amount_egp,
                fraud_count
            )
            SELECT
                DATE(event_time)        AS summary_date,
                merchant_id,
                service_type,
                COUNT(*)                AS total_transactions,
                SUM(amount_egp)         AS total_amount_egp,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
            FROM transactions
            WHERE DATE(event_time) = CURRENT_DATE
              AND status = 'success'
            GROUP BY DATE(event_time), merchant_id, service_type
            ON CONFLICT (summary_date, merchant_id)
            DO UPDATE SET
                total_transactions = EXCLUDED.total_transactions,
                total_amount_egp   = EXCLUDED.total_amount_egp,
                fraud_count        = EXCLUDED.fraud_count;
        """)
        conn.commit()

        cur.execute("""
            SELECT COUNT(*) FROM merchant_daily_summary
            WHERE summary_date = CURRENT_DATE;
        """)
        row_count = cur.fetchone()[0]
        context.log.info(
            f"Merchant reconciliation complete. "
            f"{row_count} merchant records upserted for today."
        )

    except Exception as e:
        conn.rollback()
        context.log.error(f"Merchant reconciliation failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

# Op 2: Governorate daily summary
# Reads all transactions for today and aggregates by governorate
@op
def run_governorate_summary(context: OpExecutionContext):
    context.log.info("Starting governorate daily summary")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO governorate_daily_summary (
                summary_date,
                governorate,
                total_transactions,
                total_amount_egp,
                fraud_count
            )
            SELECT
                DATE(event_time)        AS summary_date,
                governorate,
                COUNT(*)                AS total_transactions,
                SUM(amount_egp)         AS total_amount_egp,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
            FROM transactions
            WHERE DATE(event_time) = CURRENT_DATE
              AND status = 'success'
            GROUP BY DATE(event_time), governorate
            ON CONFLICT (summary_date, governorate)
            DO UPDATE SET
                total_transactions = EXCLUDED.total_transactions,
                total_amount_egp   = EXCLUDED.total_amount_egp,
                fraud_count        = EXCLUDED.fraud_count;
        """)
        conn.commit()

        cur.execute("""
            SELECT COUNT(*) FROM governorate_daily_summary
            WHERE summary_date = CURRENT_DATE;
        """)
        row_count = cur.fetchone()[0]
        context.log.info(
            f"Governorate summary complete. "
            f"{row_count} governorate records upserted for today."
        )

    except Exception as e:
        conn.rollback()
        context.log.error(f"Governorate summary failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

# Jobs
@job
def merchant_reconciliation_job():
    run_merchant_reconciliation()

@job
def governorate_summary_job():
    run_governorate_summary()

# Schedules — run both jobs daily at midnight UTC
merchant_schedule = ScheduleDefinition(
    job=merchant_reconciliation_job,
    cron_schedule="0 0 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

governorate_schedule = ScheduleDefinition(
    job=governorate_summary_job,
    cron_schedule="0 0 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

# Definitions
defs = Definitions(
    jobs=[merchant_reconciliation_job, governorate_summary_job],
    schedules=[merchant_schedule, governorate_schedule],
)