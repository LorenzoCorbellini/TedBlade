import requests
from bs4 import BeautifulSoup
from re import sub

import time

fallback_img = "https://tedblade-public-assets.s3.us-east-1.amazonaws.com/default-avatar.jpg"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# Scrape TED.com to get the thumbnail from a speaker's profile page
def get_thumbnail(url):
  response = session.get(url, headers=headers)
  if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait = 10  # Default wait value
            
            if retry_after:
                try:
                    # If we get a reyt_after, we wait for that
                    wait = int(retry_after)
                    print(f"\n[!] Rate limited (429) for {wait}s...")
                except ValueError:
                    # If it's a date, wait 30s to be sure
                    wait = 30
                    print(f"\n[!] Rate limited (429) with date: {retry_after}. Wait {wait}s...")
            else:
                print(f"\n[!] Rate limited (429) without info. Wait {wait}s...")
            
            time.sleep(wait)
            
            # Riprova la richiesta una seconda volta dopo l'attesa
            response = session.get(url, timeout=10)
  if response.status_code == 200:
      soup = BeautifulSoup(response.text, 'lxml')
      
      thumbnail = soup.find('img', class_='thumb__image')
      if thumbnail:
        src = thumbnail.get('src')
        return src
      else:
        return fallback_img
  else:
      return fallback_img

def format_speaker_name(full_name):
  name_stripped = full_name.strip()
  return sub(r'[ \-.]', '_', name_stripped)

def main():
  speaker = "Speaker che non esiste"
  name_formatted = format_speaker_name(speaker)
  url = f"https://www.ted.com/speakers/{name_formatted}"
  print(f"Getting profile {url}")
  print(get_thumbnail(url))

if __name__ == "__main__":
    main()
