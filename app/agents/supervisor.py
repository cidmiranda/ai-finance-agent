from langchain_core.messages import SystemMessage, HumanMessage

from app.services.claude_service import llm

_SYSTEM = SystemMessage(content=(
    "You are a Chief Risk Officer synthesizing reports from your specialist team "
    "on a financial reconciliation incident. Produce a clear executive summary "
    "with a definitive action recommendation. Be concise and decisive."
))


async def run(data: dict, risk: str, compliance: str, operations: str) -> str:
    response = await llm.ainvoke([
        _SYSTEM,
        HumanMessage(content=(
            f"Reconciliation data:\n"
            f"  Exchange: ${data['exchange_balance']:,.2f} | "
            f"Blockchain: ${data['blockchain_balance']:,.2f} | "
            f"Difference: ${data['difference']:,.2f} | "
            f"Risk: {data['risk_level']}\n\n"
            f"Risk Analyst:\n{risk}\n\n"
            f"Compliance Officer:\n{compliance}\n\n"
            f"Operations Specialist:\n{operations}\n\n"
            "Write a 3-4 sentence executive summary with a single clear "
            "action recommendation: APPROVE, REJECT, or ESCALATE."
        )),
    ])
    return response.content
