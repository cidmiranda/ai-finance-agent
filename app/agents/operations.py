from langchain_core.messages import SystemMessage, HumanMessage

from app.services.claude_service import llm

_SYSTEM = SystemMessage(content=(
    "You are a blockchain operations specialist with deep knowledge of exchange "
    "infrastructure, on-chain settlement, and reconciliation workflows. "
    "Diagnose root causes of balance discrepancies and recommend concrete "
    "remediation steps. Be specific and actionable."
))


async def run(data: dict) -> str:
    response = await llm.ainvoke([
        _SYSTEM,
        HumanMessage(content=(
            f"Exchange balance: ${data['exchange_balance']:,.2f}\n"
            f"Blockchain balance: ${data['blockchain_balance']:,.2f}\n"
            f"Discrepancy: ${data['difference']:,.2f}\n"
            f"Risk level: {data['risk_level']}\n\n"
            "Provide: (1) the 2-3 most likely root causes for this discrepancy, "
            "(2) recommended immediate remediation steps. Keep it to 4-5 sentences."
        )),
    ])
    return response.content
