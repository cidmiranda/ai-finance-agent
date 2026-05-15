import asyncio
import json

from langgraph.types import interrupt

from app.workflows.state import (
    WorkflowState,
)

from app.services.claude_service import llm

from app.services.telegram_service import (
    send_approval_message,
)

from app.services.approval_service import (
    update_workflow,
)

from app.mcp_client.client import (
    get_mcp_tools,
)

from app.telemetry.tracer import get_tracer
from app.kafka import producer as kafka_producer

tracer = get_tracer()
from app.kafka.topics import (
    RECONCILIATION_COMPLETED,
    RECONCILIATION_APPROVAL_REQUESTED,
    RECONCILIATION_APPROVED,
    RECONCILIATION_REJECTED,
    RECONCILIATION_AUTO_APPROVED,
)


def extract_mcp_result(result):

    if isinstance(result, list):

        text = result[0]["text"]

        try:
            return json.loads(text)

        except Exception:
            return text

    return result


def extract_balance(result) -> float:

    parsed = extract_mcp_result(result)

    if isinstance(parsed, dict):
        return float(parsed["balance"])

    return float(parsed)


async def reconciliation_agent(
    state: WorkflowState,
):

    with tracer.start_as_current_span("reconciliation_agent") as span:
        span.set_attribute("workflow.id", state.get("workflow_id", ""))

        tools = await get_mcp_tools()

        tool_map = {tool.name: tool for tool in tools}

        (
            exchange_balance_response,
            blockchain_balance_response,
        ) = await asyncio.gather(
            tool_map["get_exchange_balance"].ainvoke({}),
            tool_map["get_blockchain_balance"].ainvoke({}),
        )

        exchange_balance = extract_balance(exchange_balance_response)
        blockchain_balance = extract_balance(blockchain_balance_response)

        reconciliation_response = await tool_map["reconcile_balances"].ainvoke({
            "exchange_balance": exchange_balance,
            "blockchain_balance": blockchain_balance,
        })

        reconciliation = extract_mcp_result(reconciliation_response)

        span.set_attribute("reconciliation.exchange_balance", exchange_balance)
        span.set_attribute("reconciliation.blockchain_balance", blockchain_balance)
        span.set_attribute("reconciliation.difference", reconciliation["difference"])
        span.set_attribute("reconciliation.risk_level", reconciliation["risk_level"])
        span.set_attribute("reconciliation.requires_approval", reconciliation["requires_approval"])

        prompt = f"""
    Analyze this financial reconciliation.

    Exchange balance:
    {exchange_balance}

    Blockchain balance:
    {blockchain_balance}

    Difference:
    {reconciliation['difference']}

    Risk level:
    {reconciliation['risk_level']}
    """

        analysis = await llm.ainvoke(prompt)

        next_state = {
            **state,
            "workflow_id": state.get("workflow_id"),
            "exchange_balance": exchange_balance,
            "blockchain_balance": blockchain_balance,
            "difference": reconciliation["difference"],
            "risk_level": reconciliation["risk_level"],
            "requires_approval": reconciliation["requires_approval"],
            "analysis": analysis.content,
        }

        await kafka_producer.publish(RECONCILIATION_COMPLETED, {
            "workflow_id": next_state["workflow_id"],
            "exchange_balance": exchange_balance,
            "blockchain_balance": blockchain_balance,
            "difference": reconciliation["difference"],
            "risk_level": reconciliation["risk_level"],
            "requires_approval": reconciliation["requires_approval"],
        })

        return next_state


async def human_approval_node(
    state: WorkflowState,
):

    with tracer.start_as_current_span("human_approval") as span:
        span.set_attribute("workflow.id", state.get("workflow_id", ""))
        span.set_attribute("reconciliation.difference", state.get("difference", 0))
        span.set_attribute("reconciliation.risk_level", state.get("risk_level", ""))

        await send_approval_message(
            workflow_id=state.get("workflow_id"),
            difference=state.get("difference"),
            risk_level=state.get("risk_level"),
        )

        update_workflow(
            state.get("workflow_id"),
            {**state, "status": "WAITING_APPROVAL"},
        )

        await kafka_producer.publish(RECONCILIATION_APPROVAL_REQUESTED, {
            "workflow_id": state.get("workflow_id"),
            "difference": state.get("difference"),
            "risk_level": state.get("risk_level"),
        })

        return {**state, "status": "WAITING_APPROVAL"}


async def wait_for_approval_node(
    state: WorkflowState,
):

    decision = interrupt("Waiting for human approval")

    approved = decision.get("approved", False)
    status = "APPROVED" if approved else "REJECTED"

    with tracer.start_as_current_span("finalize_approval") as span:
        span.set_attribute("workflow.id", state.get("workflow_id", ""))
        span.set_attribute("workflow.approved", approved)
        span.set_attribute("workflow.status", status)

        update_workflow(
            state.get("workflow_id"),
            {"approved": approved, "status": status},
        )

        topic = RECONCILIATION_APPROVED if approved else RECONCILIATION_REJECTED
        await kafka_producer.publish(topic, {
            "workflow_id": state.get("workflow_id"),
            "approved": approved,
            "status": status,
        })

    return {**state, "approved": approved, "status": status}


async def auto_approve_node(
    state: WorkflowState,
):

    with tracer.start_as_current_span("auto_approve") as span:
        span.set_attribute("workflow.id", state.get("workflow_id", ""))
        span.set_attribute("workflow.approved", True)
        span.set_attribute("workflow.status", "AUTO_APPROVED")

        update_workflow(
            state.get("workflow_id"),
            {"approved": True, "status": "AUTO_APPROVED"},
        )

        await kafka_producer.publish(RECONCILIATION_AUTO_APPROVED, {
            "workflow_id": state.get("workflow_id"),
            "approved": True,
            "status": "AUTO_APPROVED",
        })

        return {
            **state,
            "workflow_id": state.get("workflow_id"),
            "approved": True,
            "status": "AUTO_APPROVED",
        }