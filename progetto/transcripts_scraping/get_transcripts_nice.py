# Un solo thread per scaricare un file alla volta
# Rispettiamo i rate limit dell'api di TED
# Usiamo truncated exponential backoff per evitare di spammare l'api

# per ottenere la request da TED: https://curlconverter.com/json/

import requests as r
import json, random, time

TRANSCRIPTS_DIR = '../data/transcripts/'

# return a response object
def download_trans(slug) -> dict:
    cookies = {
        'gbuuid': '3ddb330d-0c77-455a-b4b7-dce79967f4e1',
        '_vwo_uuid_v2': 'D662843202934C57B9FA2B3854DB21D88|ccc228244680299560026a79488e47c8',
        '_vwo_uuid': 'D662843202934C57B9FA2B3854DB21D88',
        '_vis_opt_s': '1%7C',
        'fundraiseup_cid': '17739107160443475694',
        '__stripe_mid': '0c514dbf-bc4c-40b8-82fc-26af682facf73d8c57',
        '_ted_user_id': '52350803',
        'ted_session': 'eyJ1c2VySW5mbyI6eyJmaXJzdE5hbWUiOiJhd2QiLCJsYXN0TmFtZSI6ImF3%0AZGF3ZCIsInVzZXJJRCI6NTIzNTA4MDMsImVtYWlsIjoiYm90dGlnbGlhZGl2%0AaW5vdnVvdGFAZ21haWwuY29tIiwiZ2VuZXJhdGVkX2F0IjoxNzczOTEyMDQw%0AfSwic2lnbmF0dXJlIjoiZDViNzYwZThjODQyYWY1NWU0NTgzYmEzYTAwNjUx%0AMDY2ZTZlNjdkMyJ9%0A',
        'mp_uuid': '52350803',
        'OptanonConsent': 'groups=C0001%3A1%2CC0002%3A0%2CC0004%3A0%2CC0003%3A0',
        'fundraiseup_func': '{%22t%22:%22.ted.com%22%2C%22s%22:%221779386057677%22%2C%22sp%22:4}',
        'OptanonAlertBoxClosed': 'Thu, 21 May 2026 19:17:36 GMT',
        'muxData': '=undefined&mux_viewer_id=eb8d670a-1678-41cd-8a20-e0a50562f9c0&msn=0.40485906319881115&sid=ad31b4e6-f9f0-4847-9789-c7384bf8cd9c&sst=1779391057308&sex=1779392557309',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.8',
        'client-id': 'Zenith production',
        'content-type': 'application/json',
        'origin': 'https://www.ted.com',
        'priority': 'u=1, i',
        'referer': 'https://www.ted.com/talks/' + slug,
        'sec-ch-ua': '"Chromium";v="148", "Brave";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
        'x-operation-name': 'Transcript',
        # 'cookie': 'gbuuid=3ddb330d-0c77-455a-b4b7-dce79967f4e1; _vwo_uuid_v2=D662843202934C57B9FA2B3854DB21D88|ccc228244680299560026a79488e47c8; _vwo_uuid=D662843202934C57B9FA2B3854DB21D88; _vis_opt_s=1%7C; fundraiseup_cid=17739107160443475694; __stripe_mid=0c514dbf-bc4c-40b8-82fc-26af682facf73d8c57; _ted_user_id=52350803; ted_session=eyJ1c2VySW5mbyI6eyJmaXJzdE5hbWUiOiJhd2QiLCJsYXN0TmFtZSI6ImF3%0AZGF3ZCIsInVzZXJJRCI6NTIzNTA4MDMsImVtYWlsIjoiYm90dGlnbGlhZGl2%0AaW5vdnVvdGFAZ21haWwuY29tIiwiZ2VuZXJhdGVkX2F0IjoxNzczOTEyMDQw%0AfSwic2lnbmF0dXJlIjoiZDViNzYwZThjODQyYWY1NWU0NTgzYmEzYTAwNjUx%0AMDY2ZTZlNjdkMyJ9%0A; mp_uuid=52350803; OptanonConsent=groups=C0001%3A1%2CC0002%3A0%2CC0004%3A0%2CC0003%3A0; fundraiseup_func={%22t%22:%22.ted.com%22%2C%22s%22:%221779386057677%22%2C%22sp%22:4}; OptanonAlertBoxClosed=Thu, 21 May 2026 19:17:36 GMT; muxData==undefined&mux_viewer_id=eb8d670a-1678-41cd-8a20-e0a50562f9c0&msn=0.40485906319881115&sid=ad31b4e6-f9f0-4847-9789-c7384bf8cd9c&sst=1779391057308&sex=1779392557309',
    }

    json_data = {
        'operationName': 'Transcript',
        'variables': {
            'id': slug,
            'language': 'en',
        },
        'query': 'query Transcript($id: ID!, $language: String!) {\n  translation(videoId: $id, language: $language) {\n    ...TranslationInfo\n    paragraphs {\n      cues {\n        text\n        time\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  video(id: $id, language: $language) {\n    id\n    talkExtras {\n      footnotes {\n        author\n        annotation\n        date\n        linkUrl\n        source\n        text\n        timecode\n        title\n        category\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TranslationInfo on Translation {\n  id\n  language {\n    id\n    endonym\n    englishName\n    internalLanguageCode\n    rtl\n    __typename\n  }\n  reviewer {\n    id\n    profilePath\n    avatar {\n      url\n      generatedUrl(type: SVG)\n      __typename\n    }\n    name {\n      full\n      __typename\n    }\n    __typename\n  }\n  translator {\n    id\n    profilePath\n    avatar {\n      url\n      generatedUrl(type: SVG)\n      __typename\n    }\n    name {\n      full\n      __typename\n    }\n    __typename\n  }\n  __typename\n}',
    }

    return r.post('https://www.ted.com/graphql', cookies=cookies, headers=headers, json=json_data)


# transcript is the result of download_trans() (a response object from the requests module)
def save_transcript(slug, transcript):
    f = open(f"{TRANSCRIPTS_DIR}{slug}.json", "w")
    f.write(json.dumps(transcript.json()))
    f.close()

def main():  
    slugs = []
    with open('../data/slugs.json', 'r') as f:
        slugs = json.load(f)

    MAX_RETRIES = 5
    BASE_BACKOFF = 2  # Tempo base di attesa iniziale in secondi

    for slug in slugs['slugs']:
        attempt = 0
        success = False

        while attempt < MAX_RETRIES and not success:
            try:
                response = download_trans(slug)
                
                # Gestione Rate Limit (HTTP 429)
                if response.status_code == 429:
                    attempt += 1
                    retry_after = response.headers.get("Retry-After")
                    
                    if retry_after:
                        wait_time = int(retry_after)
                        print(f"[429] Rate limit per {slug}. Il server impone di aspettare {wait_time}s.")
                    else:
                        # Formula Exponential Backoff + Jitter
                        wait_time = (BASE_BACKOFF * (2 ** attempt)) + random.uniform(0, 1)
                        print(f"[429] Rate limit per {slug}. Tentativo {attempt}/{MAX_RETRIES}. Aspetto {wait_time:.2f}s.")
                    
                    time.sleep(wait_time)
                    continue  # Salta il resto del ciclo e riprova lo stesso slug
                
                # Gestione altri errori HTTP (es. 500, 503)
                elif response.status_code != 200:
                    attempt += 1
                    wait_time = (BASE_BACKOFF * (2 ** attempt)) + random.uniform(0, 1)
                    print(f"[{response.status_code}] Errore server per {slug}. Riprovo tra {wait_time:.2f}s.")
                    time.sleep(wait_time)
                    continue

                # Se siamo qui, il codice è 200 (Successo)
                save_transcript(slug, transcript=response)
                print(f"Done: {slug}")
                success = True
                
                # Un piccolo delay di cortesia fisso per non tempestare l'API anche se va tutto bene
                time.sleep(random.uniform(1, 2))

            except r.exceptions.RequestException as e:
                # Gestione errori di rete puri (connessione caduta, timeout ecc.)
                attempt += 1
                if attempt >= MAX_RETRIES:
                    print(f"Falliti tutti i tentativi per {slug} a causa di errori di rete.")
                    raise e
                
                wait_time = (BASE_BACKOFF * (2 ** attempt)) + random.uniform(0, 1)
                print(f"Errore di rete per {slug}: {e}. Riprovo tra {wait_time:.2f}s.")
                time.sleep(wait_time)(sleep)

if __name__ == "__main__":
    main()