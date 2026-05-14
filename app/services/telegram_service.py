import requests

from app.config.settings import TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN


TELEGRAM_API = (
    f"https://api.telegram.org/bot"
    f"{TELEGRAM_BOT_TOKEN}"
)


async def send_approval_message(
    workflow_id: str,
    difference: float,
    risk_level: str,
):

    message = f"""
🚨 High Risk Reconciliation

Workflow:
{workflow_id}

Difference:
${difference}

Risk:
{risk_level}

Approve:
POST /approvals/{workflow_id}/approve

Reject:
POST /approvals/{workflow_id}/reject
"""

    requests.post(
        f"{TELEGRAM_API}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
        },
    )