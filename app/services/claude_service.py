from langchain_anthropic import ChatAnthropic
from app.config.settings import ANTHROPIC_API_KEY

llm = ChatAnthropic(
    model="claude-haiku-4-5",
    anthropic_api_key=ANTHROPIC_API_KEY,
    temperature=0
)