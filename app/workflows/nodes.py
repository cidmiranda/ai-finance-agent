from app.workflows.state import WorkflowState
from app.services.claude_service import (
    llm_with_tools,
    tools
)


async def reconciliation_agent(state: WorkflowState):

    prompt = """
    Retrieve balances using available tools
    and reconcile them.
    """

    response = await llm_with_tools.ainvoke(prompt)

    tool_results = {}

    if response.tool_calls:

        for tool_call in response.tool_calls:

            tool_name = tool_call["name"]

            selected_tool = next(
                t for t in tools
                if t.name == tool_name
            )

            result = await selected_tool.ainvoke(
                tool_call["args"]
            )

            tool_results[tool_name] = result

    exchange_balance = tool_results[
        "get_exchange_balance"
    ]

    blockchain_balance = tool_results[
        "get_blockchain_balance"
    ]

    difference = abs(
        exchange_balance - blockchain_balance
    )

    risk_level = "low"

    if difference > 100:
        risk_level = "medium"

    if difference > 1000:
        risk_level = "high"

    return {
        "exchange_balance": exchange_balance,
        "blockchain_balance": blockchain_balance,
        "difference": difference,
        "risk_level": risk_level,
        "requires_approval": difference > 100
    }