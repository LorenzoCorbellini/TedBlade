import json, os, pathlib

def main():
    transs = os.listdir(path='../../data/transcripts/')
    for trans in transs:
        print(trans)
        path = pathlib.Path(f'../../data/transcripts/{trans}')
        with open(path, 'r+') as f:
            json_data = json.load(f)
            json_data['slug'] = path.stem
            f.seek(0)
            json.dump(json_data, f, indent=4)

if __name__ == "__main__":
    main()