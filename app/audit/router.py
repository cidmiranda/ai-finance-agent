from fastapi import APIRouter

from app.audit.logger import get_audit_trail, verify_record

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/")
def list_audit_logs(workflow_id: str | None = None):
    records = get_audit_trail(workflow_id)
    return {"count": len(records), "records": records}


@router.get("/{workflow_id}")
def workflow_audit_trail(workflow_id: str):
    records = get_audit_trail(workflow_id)
    integrity_valid = all(verify_record(r) for r in records)
    return {
        "workflow_id": workflow_id,
        "count": len(records),
        "integrity_valid": integrity_valid,
        "records": records,
    }
