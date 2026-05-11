from langchain_core.tools import tool


@tool
async def get_exchange_balance() -> float:
    """
    Returns exchange wallet balance.
    """

    return 10000


@tool
async def get_blockchain_balance() -> float:
    """
    Returns blockchain wallet balance.
    """

    return 9700