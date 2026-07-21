import os, csv
from pymongo import MongoClient
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

with open('thumbnails.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    headers = next(reader) # We disregard the headers
    
    for line in reader:
      doc_id = line['_id']
      collection.update_one(
        { '_id': line['_id'] },
        { '$set': { 'thumbnail_url': line['thumbnail url'] } }
      )
      print(f'Updated url for {line['speaker full name']}')

print('done')