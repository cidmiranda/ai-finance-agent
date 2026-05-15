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

    print("RECONCILIATION STATE:", state)

    tools = await get_mcp_tools()

    tool_map = {
        tool.name: tool
        for tool in tools
    }

    (
        exchange_balance_response,
        blockchain_balance_response,
    ) = await asyncio.gather(
        tool_map["get_exchange_balance"].ainvoke({}),
        tool_map["get_blockchain_balance"].ainvoke({}),
    )

    exchange_balance = extract_balance(
        exchange_balance_response
    )

    blockchain_balance = extract_balance(
        blockchain_balance_response
    )

    reconciliation_response = await tool_map[
        "reconcile_balances"
    ].ainvoke({
        "exchange_balance": exchange_balance,
        "blockchain_balance": blockchain_balance,
    })

    reconciliation = extract_mcp_result(
        reconciliation_response
    )

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

    return {
        **state,
        "workflow_id": state.get("workflow_id"),
        "exchange_balance": exchange_balance,
        "blockchain_balance": blockchain_balance,
        "difference": reconciliation[
            "difference"
        ],
        "risk_level": reconciliation[
            "risk_level"
        ],
        "requires_approval": reconciliation[
            "requires_approval"
        ],
        "analysis": analysis.content,
    }


async def human_approval_node(
    state: WorkflowState,
):

    print("HUMAN APPROVAL STATE:", state)

    await send_approval_message(
        workflow_id=state.get("workflow_id"),
        difference=state.get("difference"),
        risk_level=state.get("risk_level"),
    )

    update_workflow(
        state.get("workflow_id"),
        {**state, "status": "WAITING_APPROVAL"},
    )

    return {**state, "status": "WAITING_APPROVAL"}


async def wait_for_approval_node(
    state: WorkflowState,
):

    print("WAIT FOR APPROVAL STATE:", state)

    decision = interrupt("Waiting for human approval")

    approved = decision.get("approved", False)
    status = "APPROVED" if approved else "REJECTED"

    update_workflow(
        state.get("workflow_id"),
        {"approved": approved, "status": status},
    )

    return {**state, "approved": approved, "status": status}


async def auto_approve_node(
    state: WorkflowState,
):

    print("AUTO APPROVE STATE:", state)

    update_workflow(
        state.get("workflow_id"),
        {
            "approved": True,
            "status": "AUTO_APPROVED",
        },
    )

    return {
        **state,
        "workflow_id": state.get("workflow_id"),
        "approved": True,
        "status": "AUTO_APPROVED",
    }