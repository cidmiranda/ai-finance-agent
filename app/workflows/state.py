from typing import TypedDict


class WorkflowState(
    TypedDict,
    total=False,
):

    workflow_id: str

    graph_type: str

    exchange_balance: float

    blockchain_balance: float

    difference: float

    risk_level: str

    requires_approval: bool

    analysis: str

    risk_analysis: str

    compliance_analysis: str

    recommendations: str

    executive_summary: str

    approved: bool

    status: str