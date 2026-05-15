import hmac
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.config.settings import AUDIT_HMAC_KEY

AUDIT_LOG_PATH = Path("audit.log")


def _sign(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(AUDIT_HMAC_KEY, payload, hashlib.sha256).hexdigest()


def log_event(
    event_type: str,
    workflow_id: str,
    actor: str,
    data: dict,
) -> dict:
    record = {
        "event_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "workflow_id": workflow_id,
        "actor": actor,
        "data": data,
    }
    record["integrity_hash"] = _sign({k: v for k, v in record.items()})

    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    return record


def get_audit_trail(workflow_id: str | None = None) -> list[dict]:
    if not AUDIT_LOG_PATH.exists():
        return []
    records = []
    with AUDIT_LOG_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if workflow_id is None or record.get("workflow_id") == workflow_id:
                records.append(record)
    return records


def verify_record(record: dict) -> bool:
    provided = record.get("integrity_hash", "")
    data = {k: v for k, v in record.items() if k != "integrity_hash"}
    expected = _sign(data)
    return hmac.compare_digest(provided, expected)
