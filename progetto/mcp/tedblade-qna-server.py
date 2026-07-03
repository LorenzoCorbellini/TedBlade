from mcp.server.fastmcp import FastMCP
from motor.motor_asyncio import AsyncIOMotorClient
from mcp.server.transport_security import TransportSecuritySettings

import boto3
from botocore.exceptions import ClientError

from json import loads

def get_secret():
    secret_name = "MongoBD"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return loads(secret)

# --- MongoDB Atlas connection ---
secret = get_secret()
username = secret["username"]
password = secret["password"]
MONGO_URI = (
    f"mongodb+srv://{username}:{password}"
    "@cluster0.hduxclv.mongodb.net/?appName=Cluster0"
)

client = AsyncIOMotorClient(MONGO_URI)
db = client["unibg_tedx_2026"]
speakers = db["speakers_full_data"]
talks = db["talks_full_data"]
transcripts = db["transcripts_flat"]

# --- MCP server ---
mcp = FastMCP(
    "tedx-server",
    host="0.0.0.0",
    port=8443,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False,),
    )


# ============================================================
# TOOLS
# ============================================================

@mcp.tool()
async def search_by_tag(tag: str, limit: int = 5) -> list[dict]:
    """Search TEDx talks that contain a given tag (e.g. 'culture', 'media')."""
    cursor = talks.find(
        {"tags": tag.lower()},
        {"_id": 0, "title": 1, "speakers": 1, "url": 1, "tags": 1, "duration": 1},
    ).limit(limit)
    return await cursor.to_list(length=limit)


@mcp.tool()
async def search_by_speaker(speaker: str, limit: int = 5) -> list[dict]:
    """Find talks by speaker name (case-insensitive partial match)."""
    cursor = talks.find(
        {"speakers": {"$regex": speaker, "$options": "i"}},
        {"_id": 0, "title": 1, "speakers": 1, "url": 1, "publishedAt": 1},
    ).limit(limit)
    return await cursor.to_list(length=limit)


@mcp.tool()
async def search_by_keyword(keyword: str, limit: int = 5) -> list[dict]:
    """Search talks by keyword in title or description."""
    cursor = talks.find(
        {
            "$or": [
                {"title": {"$regex": keyword, "$options": "i"}},
                {"description": {"$regex": keyword, "$options": "i"}},
            ]
        },
        {"_id": 0, "title": 1, "speakers": 1, "url": 1, "description": 1},
    ).limit(limit)
    return await cursor.to_list(length=limit)


@mcp.tool()
async def get_talk(slug: str) -> dict:
    """Get full details for a single talk by its slug."""
    result = await talks.find_one({"slug": slug}, {"_id": 0})
    return result or {"error": f"No talk found with slug '{slug}'"}

@mcp.tool()
async def get_speaker(speaker: str) -> dict:
    """Get full details for a single speaker by its name (case-insensitive full match)."""
    result = await speakers.find_one({"speaker": {"$regex": speaker, "$options": "i"}}, {"_id": 0})
    return result or {"error": f"No speaker found with name '{speaker}'"}

@mcp.tool()
async def get_transcript(slug: str) -> dict:
    """Get full transcript of a talk by its slug."""
    result = await transcripts.find_one({"slug": slug}, {"_id": 0})
    return result or {"error": f"No transcript found for talk with slug '{slug}'"}

@mcp.tool()
async def top_tags(limit: int = 10) -> list[dict]:
    """Return the most common tags across all talks."""
    pipeline = [
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "tag": "$_id", "count": 1}},
    ]
    return await talks.aggregate(pipeline).to_list(length=limit)


# ============================================================
# RESOURCES
# ============================================================

@mcp.resource("tedx://speakers_schema")
async def get_speakers_schema() -> str:
    """Expose the TEDx speakrs collection schema as a resource."""
    return """
    Collection: unibg_tedx_2026.speakers_full_data
    Fields:
      - _id: string               (unique identifier of the document)
      - speaker: string           (speaker name)
      - talks: array[object]      (list of talks by this speaker)
        Each talk object contains:
          - title: string         (talk title)
          - slug: string          (unique identifier of the talk)
          - url: string           (URL of the talk)
          - duration: string|null (length in seconds)
          - publishedAt: string|null (ISO date)
          - statistics: object    (view counts and engagement stats)
            - viewCount_ted: string|null     (views on TED.com)
            - viewCount_yt: string|null      (views on YouTube)
            - likeCount_yt: string|null      (likes on YouTube)
            - commentCount_yt: string|null   (comments on YouTube)
            - favoriteCount_yt: string|null  (favorites on YouTube)
    """

@mcp.resource("tedx://talks_schema")
async def get_talks_schema() -> str:
    """Expose the TEDx talks collection schema as a resource."""
    return """
    Collection: unibg_tedx_2026.talks_full_data
    Fields:
      - _id: string               (unique identifier of the document)
      - title: string             (talk title)
      - speakers: string|null     (speaker name)
      - slug: string              (unique identifier of the talk)
      - url: string               (TED.com URL)
      - description: string       (full description)
      - duration: string|null     (length in seconds)
      - publishedAt: string|null  (ISO date)
      - yt_id: string|null        (YouTube Video ID)
      - watch_next: string|null   (slug of the recommended next talk)
      - tags: array[string]|null  (topic tags)
      - statistics: object        (view counts and engagement stats)
        - viewCount_ted: string|null     (views on TED.com)
        - viewCount_yt: string|null      (views on YouTube)
        - likeCount_yt: string|null      (likes on YouTube)
        - commentCount_yt: string|null   (comments on YouTube)
        - favoriteCount_yt: string|null  (favorites on YouTube)
    """

@mcp.resource("tedx://transcripts_schema")
async def get_transcripts_schema() -> str:
    """Expose the TEDx transcripts collection schema as a resource."""
    return """
    Collection: unibg_tedx_2026.transcripts_flat
    Fields:
      - _id: string               (unique identifier of the document)
      - slug: string              (unique identifier matching the related talk)
      - data: object              (container for the translation data)
        - translation: object     (details of the translation)
          - language: string      (language, e.g. 'English')
          - cues: array[object]   (list of transcript fragments)
            Each cue object contains:
              - text: string      (the spoken text fragment)
              - timestamp: int    (the timestamp in milliseconds)
    """


@mcp.resource("tedx://speakers_stats")
async def get_speakers_stats() -> str:
    """Basic stats about the TEDx speakers collection."""
    total = await speakers.count_documents({})
    return f"Total speakers in dataset: {total}"

@mcp.resource("tedx://talks_stats")
async def get_talks_stats() -> str:
    """Basic stats about the TEDx talks collection."""
    total = await talks.count_documents({})
    return f"Total talks in dataset: {total}"


# ============================================================
# PROMPTS
# ============================================================

@mcp.prompt()
def recommend_prompt(topic: str) -> str:
    """Template prompt to recommend talks on a given topic."""
    return (
        f"Use the `search_by_tag` or `search_by_keyword` tool to find TEDx talks "
        f"about '{topic}'. Then summarize the 3 most relevant ones, including "
        f"speaker, title and a one-line takeaway. Provide the URLs at the end."
    )


@mcp.prompt()
def speaker_profile_prompt(speaker: str) -> str:
    """Template prompt to build a comprehensive profile of a speaker, including stats, performance, and deep transcript analysis."""
    return (
        f"You are an expert biographer and research assistant.\n"
        f"1. First, search for talks by '{speaker}' using `search_by_speaker`. "
        f"Also, use `get_speaker` to see the structured data available for this speaker.\n"
        f"2. Check the `tedx://speakers_stats` resource to understand the general context of speakers in the dataset.\n"
        f"3. For the talks you find, use the `get_transcript` tool with their corresponding 'slug' to read the actual spoken text.\n"
        f"4. Synthesize all this information into a detailed profile. Your report must include:\n"
        f"   - An overview of the speaker's presence and engagement metrics in the dataset.\n"
        f"   - The main themes, core arguments, and recurring ideas extracted directly from the text of their transcripts.\n"
        f"   - An analysis of their communication style and rhetorical approach based on how they deliver their ideas."
    )

@mcp.prompt()
def analyze_topic_performance_prompt(topic: str) -> str:
    """Template to find talks on a topic and analyze their social media engagement."""
    return (
        f"You are a Data Analytics expert.\n"
        f"1. Use the `search_by_keyword` or `search_by_tag` tool to find talks related to '{topic}'.\n"
        f"2. For the results found, use the `get_talk` tool with their corresponding 'slug' to inspect the full `statistics` object (YouTube vs. TED views, likes, comments).\n"
        f"3. Generate a comprehensive report explaining which talk achieved the most success and why, "
        f"highlighting if there is strong user engagement (the ratio between views and comments/likes)."
    )

@mcp.prompt()
def find_topic_timestamps_prompt(topic: str) -> str:
    """Template prompt to locate specific moments/timestamps where a topic is discussed within a talk's transcript."""
    return (
        f"You are a precise video editor and content curator.\n"
        f"1. Use `search_by_keyword` to find the most relevant TEDx talk related to '{topic}'.\n"
        f"2. Take the 'slug' of the best matching talk and retrieve its full text using the `get_transcript` tool.\n"
        f"3. Carefully scan the array of `cues` inside the transcript data. Identify the exact moments where the speaker explicitly mentions or discusses '{topic}'.\n"
        f"4. Format the output as a timeline report including:\n"
        f"   - The title and URL of the talk you analyzed.\n"
        f"   - A bulleted list of key moments with their exact timestamp (convert the millisecond timestamp into a readable MM:SS or HH:MM:SS format) and a brief quote or summary of what is being said at that moment."
    )

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        mcp.streamable_http_app(),
        host="0.0.0.0",
        port=8443,
        ssl_keyfile="key.pem",
        ssl_certfile="cert.pem",
    )