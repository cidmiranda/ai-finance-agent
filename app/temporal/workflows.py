from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.temporal.activities import (
        fetch_balances_activity,
        reconcile_activity,
        notify_approval_activity,
        cancel_approval_activity,
        finalize_activity,
    )

_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_APPROVAL_TIMEOUT = timedelta(hours=24)


@workflow.defn
class ReconciliationSaga:

    def __init__(self):
        self._decision: bool | None = None

    @workflow.signal
    async def receive_decision(self, approved: bool):
        self._decision = approved

    @workflow.run
    async def run(self, workflow_id: str) -> dict:
        compensations: list[str] = []

        try:
            # Step 1: fetch balances (retries automatically on transient failure)
            balances = await workflow.execute_activity(
                fetch_balances_activity,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

            # Step 2: reconcile
            result = await workflow.execute_activity(
                reconcile_activity,
                args=[balances],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            result["workflow_id"] = workflow_id

            if result["requires_approval"]:
                # Step 3: notify human — register compensation before calling
                compensations.append(workflow_id)
                await workflow.execute_activity(
                    notify_approval_activity,
                    args=[result],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )

                # Wait for approval signal (up to 24h)
                await workflow.wait_condition(
                    lambda: self._decision is not None,
                    timeout=_APPROVAL_TIMEOUT,
                )
                approved = self._decision
                compensations.clear()
            else:
                approved = True

            # Step 4: finalize
            await workflow.execute_activity(
                finalize_activity,
                args=[{**result, "approved": approved}],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )

            return {
                **result,
                "approved": approved,
                "status": "APPROVED" if approved else "REJECTED",
            }

        except Exception:
            # Saga compensation: run in reverse order
            for wf_id in reversed(compensations):
                await workflow.execute_activity(
                    cancel_approval_activity,
                    args=[wf_id],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            raise
