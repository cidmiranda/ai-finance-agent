from pydantic import BaseModel

class ReconciliationResult(BaseModel):
    difference: float
    risk_level: str
    requires_approval: bool