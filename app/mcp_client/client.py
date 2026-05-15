from langchain_mcp_adapters.client import MultiServerMCPClient
import sys

client = MultiServerMCPClient(
    {
        "finance": {
            "command": sys.executable,
            "args": [
                "-m",
                "app.mcp_server.server"
            ],
            "transport": "stdio",
        }
    }
)

_tools_cache: list | None = None

async def get_mcp_tools():
    global _tools_cache
    if _tools_cache is None:
        _tools_cache = await client.get_tools()
    return _tools_cache