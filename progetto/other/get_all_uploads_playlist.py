import requests, json

OUTPUT = 'playlist_info.json'
# Your Youtube API key
API_KEY = "" 
# You can get the channel id by looking
# up on Google "get youtube channel id"
CHANNEL_ID = "UC_aEa8K-EOJ3D6gOs7HcyNg" # NCS

res = requests.get(f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}")

print(res.json())

with open(OUTPUT, 'w') as f:
    json.dump(res.json(), f)
