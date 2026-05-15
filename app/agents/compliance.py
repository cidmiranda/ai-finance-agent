from langchain_core.messages import SystemMessage, HumanMessage

from app.services.claude_service import llm

_SYSTEM = SystemMessage(content=(
    "You are a financial compliance officer with expertise in SOX, AML, and "
    "crypto exchange regulations. Review reconciliation discrepancies for potential "
    "regulatory violations, reporting obligations, and audit implications. "
    "Be precise about which regulations may apply."
))


async def run(data: dict) -> str:
    response = await llm.ainvoke([
        _SYSTEM,
        HumanMessage(content=(
            f"Exchange balance: ${data['exchange_balance']:,.2f}\n"
            f"Blockchain balance: ${data['blockchain_balance']:,.2f}\n"
            f"Discrepancy: ${data['difference']:,.2f}\n"
            f"Risk level: {data['risk_level']}\n\n"
            "Identify: (1) applicable regulations or reporting thresholds this "
            "discrepancy may trigger, (2) required documentation or escalation, "
            "(3) audit trail implications. Keep it to 3-4 sentences."
        )),
    ])
    return response.content
