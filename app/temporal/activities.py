import asyncio
import json

from temporalio import activity

from app.mcp_client.client import get_mcp_tools
from app.services.telegram_service import send_approval_message
from app.kafka import producer as kafka_producer
from app.kafka.topics import (
    RECONCILIATION_COMPLETED,
    RECONCILIATION_APPROVAL_REQUESTED,
    RECONCILIATION_APPROVED,
    RECONCILIATION_REJECTED,
    RECONCILIATION_AUTO_APPROVED,
)


def _extract_mcp_result(result):
    if isinstance(result, list):
        text = result[0]["text"]
        try:
            return json.loads(text)
        except Exception:
            return text
    return result


def _extract_balance(result) -> float:
    parsed = _extract_mcp_result(result)
    if isinstance(parsed, dict):
        return float(parsed["balance"])
    return float(parsed)


@activity.defn(name="fetch_balances")
async def fetch_balances_activity() -> dict:
    tools = await get_mcp_tools()
    tool_map = {t.name: t for t in tools}

    exchange_resp, blockchain_resp = await asyncio.gather(
        tool_map["get_exchange_balance"].ainvoke({}),
        tool_map["get_blockchain_balance"].ainvoke({}),
    )

    return {
        "exchange_balance": _extract_balance(exchange_resp),
        "blockchain_balance": _extract_balance(blockchain_resp),
    }


@activity.defn(name="reconcile")
async def reconcile_activity(balances: dict) -> dict:
    tools = await get_mcp_tools()
    tool_map = {t.name: t for t in tools}

    result = await tool_map["reconcile_balances"].ainvoke(balances)
    reconciliation = _extract_mcp_result(result)

    full = {**balances, **reconciliation}

    await kafka_producer.publish(RECONCILIATION_COMPLETED, full)

    return full


@activity.defn(name="notify_approval_requested")
async def notify_approval_activity(payload: dict) -> None:
    await send_approval_message(
        workflow_id=payload["workflow_id"],
        difference=payload["difference"],
        risk_level=payload["risk_level"],
    )

    await kafka_producer.publish(RECONCILIATION_APPROVAL_REQUESTED, {
        "workflow_id": payload["workflow_id"],
        "difference": payload["difference"],
        "risk_level": payload["risk_level"],
    })


@activity.defn(name="cancel_approval")
async def cancel_approval_activity(workflow_id: str) -> None:
    """Compensation: notifica que o workflow foi cancelado antes da aprovação."""
    await kafka_producer.publish(RECONCILIATION_REJECTED, {
        "workflow_id": workflow_id,
        "approved": False,
        "status": "CANCELLED",
    })


@activity.defn(name="finalize")
async def finalize_activity(payload: dict) -> None:
    approved = payload.get("approved", False)
    topic = RECONCILIATION_APPROVED if approved else RECONCILIATION_REJECTED

    await kafka_producer.publish(topic, {
        "workflow_id": payload["workflow_id"],
        "approved": approved,
        "status": "APPROVED" if approved else "REJECTED",
    })
