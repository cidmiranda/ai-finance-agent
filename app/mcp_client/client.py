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

async def get_mcp_tools():
    return await client.get_tools()