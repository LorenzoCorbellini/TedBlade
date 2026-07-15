from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

db_uri = f"mongodb+srv://{db_user}:{db_password}@cluster0.hduxclv.mongodb.net/?appName=Cluster0"
client = MongoClient(db_uri)

db = client["unibg_tedx_2026"]
collection = db["speakers_full_data"]

cursor = collection.find({}, { "speaker": 1 })

for document in cursor:
    speaker = document.get("speaker")
    if speaker is not None:
      thumbnail_url = "https://tedblade-public-assets.s3.us-east-1.amazonaws.com/default-avatar.jpg"
      collection.update_one(
        { "_id": document["_id"] },
        { "$set": { "thumbnail_url": thumbnail_url } }
      )

print('done')