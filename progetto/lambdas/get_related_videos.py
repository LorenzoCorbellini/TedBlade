import json, os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

db_password = os.environ['db_password']
uri = f"mongodb+srv://lcorbellini_db_user:{db_password}@cluster0.hduxclv.mongodb.net/?appName=Cluster0"

def lambda_handler(event, context):
    id = event["video_id"]

    client = MongoClient(uri, server_api=ServerApi('1'))
    database = client["Cluster0"]
    collection = database["unibg_tedx_2026"]

    try:
        results = collection.find({ "tags" : "women" })
        return {
            json.dumps(list(results))
        }
    except Exception as e:
        print(e)

