import json

from app.workflows.state import WorkflowState

from app.services.claude_service import llm

from app.mcp_client.client import (
    get_mcp_tools,
)


def extract_mcp_result(result):

    # MCP content blocks
    if isinstance(result, list):

        text = result[0]["text"]

        # JSON payload
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
    state: WorkflowState
):

    tools = await get_mcp_tools()

    tool_map = {
        tool.name: tool
        for tool in tools
    }

    # deterministic orchestration

    exchange_balance_response = await tool_map[
        "get_exchange_balance"
    ].ainvoke({})

    blockchain_balance_response = await tool_map[
        "get_blockchain_balance"
    ].ainvoke({})

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
        "blockchain_balance": blockchain_balance
    })

    # FIX HERE

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

    Explain:
    - operational risk
    - whether approval is recommended
    - possible causes
    """

    analysis = await llm.ainvoke(prompt)

    return {
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
        "analysis": analysis.content
    }