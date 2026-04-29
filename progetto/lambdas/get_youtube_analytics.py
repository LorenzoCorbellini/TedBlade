'''
Questa lambda fa quanto segue
1. legge il file matches.json da un bucket
    il file associa i video del dataset TED ai rispettivi video su YouTube (se esistono)
2. ottiene le statistiche di ciascun video YT dall'API di Google
3. salva i risultati nel file video_stats.json
4. memorizza i risultati nel bucket
'''

import requests, json, os, boto3

API_KEY = region = os.environ['YT_API_KEY']
CHANNEL_ID = "UCAuUUnT6oDeKwE6v1NGQxug"
BUCKET_NAME = "yt-data-2834"

data = []
video_stats = []

s3_client = boto3.client("s3", "us-east-1")

def lambda_handler(event, context):
    
    read_from_s3 = s3_client.get_object(
        Key="matches.json",
        Bucket="yt-data-2834"
    )
    data = json.loads(read_from_s3["Body"].read().decode("utf-8"))
    print(data)

    url = "https://www.googleapis.com/youtube/v3/videos"

#   Test with one video
    # video = data[1]
    # params = {
    #         "part": "snippet,statistics",
    #         "key": API_KEY,
    #         "id": video["yt_id"]
    # }
    # res = requests.get(url, params=params).json()
    # video_stats.append(res)

    for video in data:
        if video["yt_id"] is None:
            continue

        params = {
                "part": "snippet,statistics",
                "key": API_KEY,
                "id": video["yt_id"]
        }

        res = requests.get(url, params=params).json()
        video_stats.append(res)

    save_to_s3 = s3_client.put_object(
        Key="video_stats.json",
        Bucket="yt-data-2834",
        Body=(json.dumps(video_stats).encode("utf-8"))
    )

    print("done")
