import logging

from temporalio.client import Client
from temporalio.worker import Worker

from app.config.settings import TEMPORAL_HOST
from app.temporal.activities import (
    fetch_balances_activity,
    reconcile_activity,
    notify_approval_activity,
    cancel_approval_activity,
    finalize_activity,
)
from app.temporal.workflows import ReconciliationSaga

logger = logging.getLogger(__name__)

TASK_QUEUE = "reconciliation"


async def run_worker():
    client = await Client.connect(TEMPORAL_HOST)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ReconciliationSaga],
        activities=[
            fetch_balances_activity,
            reconcile_activity,
            notify_approval_activity,
            cancel_approval_activity,
            finalize_activity,
        ],
    )
    logger.info("Temporal worker started on task queue '%s'", TASK_QUEUE)
    await worker.run()
