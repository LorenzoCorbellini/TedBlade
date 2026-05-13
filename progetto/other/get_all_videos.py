import requests, json

# Generate this file with get_all_uploads_playlist.py
PLAYLIST_FILE = 'playlist_info.json'
# Your Youtube API key
API_KEY = ""

playlist_info = []

with open(PLAYLIST_FILE) as f:
    playlist_info = json.load(f)

uploads_id = playlist_info["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

print(uploads_id)

#### Get videos from playlist

url = "https://www.googleapis.com/youtube/v3/playlistItems"
next_page = None
videos = []

while True:
  print(f"Sending request for page {next_page}")
  params = {
        "part": "snippet",
        "playlistId": uploads_id,
        "maxResults": 50,
        "key": API_KEY,
        "pageToken": next_page
  }

  res = requests.get(url, params=params).json()
  videos.append(res)

  next_page = res.get("nextPageToken")

  if not next_page:
    break

with open("playlist.json", "w") as f:
  f.write(json.dumps(videos))

print("done")

