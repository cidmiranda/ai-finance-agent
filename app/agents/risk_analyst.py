from langchain_core.messages import SystemMessage, HumanMessage

from app.services.claude_service import llm

_SYSTEM = SystemMessage(content=(
    "You are a senior financial risk analyst specializing in crypto exchange "
    "reconciliation. Analyze discrepancies between exchange and blockchain balances "
    "and provide a concise, structured risk assessment. Focus on financial exposure, "
    "severity, and urgency. Be direct and quantitative."
))


async def run(data: dict) -> str:
    response = await llm.ainvoke([
        _SYSTEM,
        HumanMessage(content=(
            f"Exchange balance: ${data['exchange_balance']:,.2f}\n"
            f"Blockchain balance: ${data['blockchain_balance']:,.2f}\n"
            f"Discrepancy: ${data['difference']:,.2f}\n"
            f"Risk level: {data['risk_level']}\n\n"
            "Provide a 3-4 sentence risk assessment covering: financial exposure, "
            "severity classification, and whether immediate action is required."
        )),
    ])
    return response.content
