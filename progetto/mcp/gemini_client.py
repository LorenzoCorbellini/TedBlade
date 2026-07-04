import asyncio
import os
import httpx
import urllib3

from google import genai
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


SERVER_IP = "100.26.109.6"
SERVER_URL = f"https://{SERVER_IP}:8443/mcp"


def load_env():
    with open("api_key.env") as f:
        for line in f:
            if "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")


def insecure_httpx_client(**kwargs):
    return httpx.AsyncClient(
        verify=False,
        timeout=httpx.Timeout(60.0),
        follow_redirects=True,
    )


async def main():
    load_env()

    gemini = genai.Client()

    prompt = """
    Trova lo speaker TED più famoso sul cambiamento climatico
    e analizza performance e statistiche.
    """

    print("Connessione MCP...")

    async with streamablehttp_client(
        SERVER_URL,
        httpx_client_factory=insecure_httpx_client,
    ) as (read, write, _):

        async with ClientSession(read, write) as session:

            await session.initialize()

            print("Tool disponibili:")
            tools = await session.list_tools()
            print(tools)

            # =========================
            # 1. DECIDI QUALE TOOL USARE
            # =========================
            # (qui lo fai statico per evitare loop complessi)
            tool_name = tools.tools[0].name  # primo tool disponibile

            print(f"Uso tool: {tool_name}")

            # =========================
            # 2. ESEGUI TOOL MCP
            # =========================
            tool_result = await session.call_tool(
                tool_name,
                arguments={}
            )

            print("\nRisultato MCP:")
            print(tool_result)

            # =========================
            # 3. PASSA A GEMINI
            # =========================
            response = await gemini.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"""
                {prompt}

                Dati ottenuti dal server MCP:
                {tool_result}
                """,
            )

            print("\n===== RISPOSTA GEMINI =====\n")
            print(response.text)


if __name__ == "__main__":
    urllib3.disable_warnings()
    asyncio.run(main())