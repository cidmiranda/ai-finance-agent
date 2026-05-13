from app.workflows.state import (
    WorkflowState,
)


def route_after_reconciliation(
    state: WorkflowState
):

    if state["requires_approval"]:
        return "human_approval"

    return "auto_approve"