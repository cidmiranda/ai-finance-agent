from langchain_anthropic import ChatAnthropic
from app.config.settings import ANTHROPIC_API_KEY

from app.tools.finance_tools import (
    get_exchange_balance,
    get_blockchain_balance
)

llm = ChatAnthropic(
    model="claude-3-5-sonnet-latest",
    anthropic_api_key=ANTHROPIC_API_KEY,
    temperature=0
)

tools = [
    get_exchange_balance,
    get_blockchain_balance
]

llm_with_tools = llm.bind_tools(tools)