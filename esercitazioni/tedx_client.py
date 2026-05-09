"""
Minimal MCP client for testing the TEDx server.
Ignores SSL verification — for use with self-signed certs only.
University lesson demo.
"""

import asyncio
import ssl

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# CHANGE THIS to your server URL
SERVER_URL = "https://<IP>/mcp"

# Build an SSL context that does NOT verify certs (demo only!)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# httpx (used internally by the MCP client) accepts a `verify` kwarg
# via the httpx_client_factory hook
import httpx


def insecure_httpx_client(headers=None, timeout=None, auth=None):
    return httpx.AsyncClient(
        headers=headers,
        timeout=timeout if timeout else httpx.Timeout(30.0),
        auth=auth,
        verify=False,  # <-- skip cert verification
        follow_redirects=True,
    )


async def main():
    async with streamablehttp_client(
        SERVER_URL,
        httpx_client_factory=insecure_httpx_client,
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            # 1. Initialize the connection
            await session.initialize()
            print("✓ Connected to TEDx MCP server\n")

            # 2. List available tools
            tools = await session.list_tools()
            print("Available tools:")
            for t in tools.tools:
                print(f"  • {t.name} — {t.description}")
            print()

            # 3. Call a tool
            print("Calling search_by_tag(tag='feminism', limit=3)...\n")
            result = await session.call_tool(
                "search_by_tag",
                arguments={"tag": "feminism", "limit": 3},
            )
            for item in result.content:
                print(item.text)


if __name__ == "__main__":
    # Suppress the noisy InsecureRequestWarning
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    asyncio.run(main())