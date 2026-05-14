from mcp.server.fastmcp import FastMCP
import logging

from app.schemas.reconciliation import ReconciliationResult

import sys

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr
)

mcp = FastMCP("Finance MCP Server")


@mcp.tool()
async def get_exchange_balance() -> float:
    """
    Returns exchange wallet balance.
    """

    return 10000

@mcp.tool()
async def get_blockchain_balance() -> float:
    """
    Returns blockchain wallet balance.
    """

    return 9700

@mcp.tool()
async def reconcile_balances(
    exchange_balance: float,
    blockchain_balance: float
) -> ReconciliationResult:
    """
    Reconciles exchange and blockchain balances.
    """
    logging.info(
        "Running reconciliation workflow"
    )

    difference = abs(
        exchange_balance - blockchain_balance
    )

    risk_level = "low"

    if difference > 100:
        risk_level = "medium"

    if difference > 1000:
        risk_level = "high"

    return {
        "difference": difference,
        "risk_level": risk_level,
        "requires_approval": difference > 100
    }

if __name__ == "__main__":
    mcp.run(transport="stdio")