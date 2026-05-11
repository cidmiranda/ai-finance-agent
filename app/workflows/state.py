from typing import TypedDict

class WorkflowState(TypedDict):
    exchange_balance: float
    blockchain_balance: float
    difference: float
    risk_level: str
    requires_approval: bool