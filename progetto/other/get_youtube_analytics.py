import requests, json, os

API_KEY = os.environ["YT_API_KEY"]
CHANNEL_ID = "UCAuUUnT6oDeKwE6v1NGQxug" # TED
MATCHES_FILE = "matches.json"

data = []
video_stats = []

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


with open(f"../data/{MATCHES_FILE}") as f:
   data = json.load(f)

# Conta quanti video di TED hanno un corrispondente video YT

count = 0
for video in data:
   if video["yt_id"] is None:
      count = count + 1

print(f"Numero di video TED: {len(data)}")
print(f"Video senza corrispondente YT: {count}")
print(f"% video senza corrispondente YT: {count/len(data)}")

url = "https://www.googleapis.com/youtube/v3/videos"

# Test with one video
# video = data[1]
# params = {
#         "part": "snippet,statistics",
#         "key": API_KEY,
#         "id": video["yt_id"]
# }
# res = requests.get(url, params=params).json()
# video_stats.append(res)

# Per ogni video, prendi l'id su YT e ottieni le statistiche
# Lo facciamo in batch di 50 video per usare meno richieste API

s = requests.Session()
ids = [video["yt_id"] for video in data if video["yt_id"] is not None]

for id_batch in chunks(ids, 50):
	params = {
		"part": "snippet,statistics",
       "key": API_KEY,
       "id": ",".join(id_batch)
	}

	res = s.get(url, params=params)
	video_stats.extend(res.json().get("items", []))
	res.close()

with open("video_stats.json", "w") as f:
  f.write(json.dumps(video_stats))

print("done")
