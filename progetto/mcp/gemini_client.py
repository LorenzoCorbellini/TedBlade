import asyncio
import ssl
import os
from google import genai
from google.genai import types
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
import httpx
import urllib3

def load_env_file(filename="api_key.env"):
    """Manually parse a simple env file and inject keys into os.environ."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_path, filename)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing required file: {file_path}")
        
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip().strip("'\"")

def insecure_httpx_client(headers=None, timeout=None, auth=None):
    return httpx.AsyncClient(
        headers=headers,
        timeout=timeout if timeout else httpx.Timeout(45.0),
        auth=auth,
        verify=False,
        follow_redirects=True,
    )

async def main():
    # Inizializza SDK di Gemini
    try:
        load_env_file("api_key.env")
        print("API key caricata con successo da api_key.env")
    except Exception as e:
        print(f"Errore nel caricamento di api_key.env: {e}")
        return
    
    # Recupera l'IP del server dinamicamente dalle variabili d'ambiente caricate
    SERVER_IP = os.environ.get('SERVER_IP', '100.26.109.6')
    SERVER_URL = f'https://{SERVER_IP}:8443/mcp'
    
    ai_client = genai.Client()

    print(f"Connessione in corso al server remoto su AWS ({SERVER_URL})...")
    
    async with streamablehttp_client(
        SERVER_URL,
        httpx_client_factory=insecure_httpx_client,
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            
            # Inizializza la sessione del protocollo MCP
            await session.initialize()
            print("Connesso con successo al server TedBlade su AWS\n")

            # 1. Recupera i tool remoti dall'MCP Server su AWS
            mcp_tools = await session.list_tools()
            
            # 2. Converte i tool MCP nel formato "Google Tool" strutturato per l'SDK di Gemini
            gemini_tools = types.Tool(function_declarations=[
                {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema,
                }
                for tool in mcp_tools.tools
            ])
            
            user_prompt = (
                '''
                Ci sono dei talks che parlando Internet? Se ne trovi, puoi dirmi quali sono gli speaker che
                hanno tenuto questi talk? Poi elencami le statistiche (views, likes, etc.) di ogni speaker e
                identifica il più popolare. Infine, prepara un breve profilo per il più popolare, inserendo
                - una piccola sezione sulle sue statistiche 
                - quali temi affronta
                - se parla di sicurezza su internet e dove (aggiungi il timestamp)
                '''
            )
            
            print(f"\nPrompt inviato a Gemini:\n\"{user_prompt}\"\n")
            print("Elaborando...")
            print("----------------------------------------------------------------------")
            
            # Prepariamo l'array dei messaggi per gestire la conversazione ricorsiva (Loop dell'Agente)
            messages = [types.Content(role="user", parts=[types.Part.from_text(text=user_prompt)])]
            
            while True:
                # Genera il contenuto inviando la cronologia corrente e lo schema dei tool
                response = await ai_client.aio.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=messages,
                    config=types.GenerateContentConfig(
                        tools=[gemini_tools],
                        temperature=0.3
                    )
                )
                
                # Registra la risposta (o la richiesta di tool) nella cronologia dei messaggi
                if response.candidates and response.candidates[0].content:
                    messages.append(response.candidates[0].content)

                # Se Gemini richiede l'esecuzione di un tool remoto
                if response.function_calls:
                    tool_call = response.function_calls[0]
                    
                    # Log intermedio (utile per la demo per mostrare cosa fa l'agente dietro le quinte)
                    print(f"[Uso Tool]: Invocazione di '{tool_call.name}' con argomenti {dict(tool_call.args)}...")
                    
                    # Esegue il tool sul database MongoDB Atlas passando per AWS
                    mcp_result = await session.call_tool(
                        tool_call.name,
                        arguments=dict(tool_call.args)
                    )
                    
                    # Converte la risposta del database in un formato digeribile per l'LLM
                    function_response_part = types.Part.from_function_response(
                        name=tool_call.name,
                        response={"result": mcp_result.content}
                    )
                    messages.append(types.Content(role="tool", parts=[function_response_part]))
                    
                else:
                    # Quando Gemini non richiede più tool, significa che ha elaborato il report finale
                    print("\n================ RISPOSTA ================\n")
                    print(response.text)
                    print("\n===========================================================")
                    break

if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    asyncio.run(main())