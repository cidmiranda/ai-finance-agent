from pydantic import BaseModel

class ReconciliationResult(BaseModel):
    difference: float
    risk_level: str
    requires_approval: bool

class ReconciliationRequest(BaseModel):
    exchange_balance: float
    blockchain_balance: float