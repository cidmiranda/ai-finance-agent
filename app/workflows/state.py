from typing import TypedDict


class WorkflowState(
    TypedDict,
    total=False,
):

    workflow_id: str

    exchange_balance: float

    blockchain_balance: float

    difference: float

    risk_level: str

    requires_approval: bool

    analysis: str

    approved: bool

    status: str