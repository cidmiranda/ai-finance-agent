from app.workflows.state import WorkflowState

async def reconcile_node(state: WorkflowState):

    difference = abs(
        state["exchange_balance"]
        - state["blockchain_balance"]
    )

    risk_level = "low"

    if difference > 100:
        risk_level = "medium"

    if difference > 1000:
        risk_level = "high"

    return {
        "difference": difference,
        "risk_level": risk_level,
        "requires_approval": difference > 100
    }