from dagster import repository
from pipeline import (
    merchant_reconciliation_job,
    governorate_summary_job,
    merchant_schedule,
    governorate_schedule,
)

@repository
def pulse_repository():
    return [
        merchant_reconciliation_job,
        governorate_summary_job,
        merchant_schedule,
        governorate_schedule,
    ]