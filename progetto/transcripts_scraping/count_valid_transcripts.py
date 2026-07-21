import json, os

def main():
    null_items = []
    transs = os.listdir(path='../../data/transcripts/')
    for trans in transs:
        with open(f'../../data/transcripts/{trans}', 'r') as f:
            json_data = json.load(f)
            if json_data['data']['translation'] is None:
                null_items.append(trans)
    print(json.dumps(null_items))
    print(f"Talks missing transcripts: {len(null_items)}/{len(transs)}")

if __name__ == "__main__":
    main()