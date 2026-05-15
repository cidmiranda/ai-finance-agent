from langchain_mcp_adapters.client import MultiServerMCPClient

client = MultiServerMCPClient(
    {
        "finance": {
            "url": "http://localhost:8000/mcp-server/mcp",
            "transport": "streamable_http",
        }
    }
)

_tools_cache: list | None = None

async def get_mcp_tools():
    global _tools_cache
    if _tools_cache is None:
        _tools_cache = await client.get_tools()
    return _tools_cache