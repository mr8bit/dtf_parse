import json
import requests
import time
import pathlib
import os
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map  # or thread_map


def extract(file):
    res = []
    with open(str(file), "r") as read_file:
        data = json.load(read_file)
        for item in data['result']:
            res.append({
                'id': item['id'],
                'user_id':item['author']['id'],
                'date':item['date'],
                'text': item['text'],
                'replyTo': item['replyTo'],
                'entry_id': item['entry']['id'],
                'entry_url': item['entry']['url'],
            })
    return res

def read_all_comments(user_id):
    p = pathlib.Path(f"./comments/{user_id}")
    comm_file = p.glob("*.json")
    res = []
    for file in comm_file:
        res.extend(extract(str(file)))
    return res


def clear(user_id):
    p = pathlib.Path(f"./comments/{user_id}")
    comm_file = p.glob("*.json")
    for file in comm_file:
        os.remove(str(file))


def convert_to_csv(user_id):
    new_com = read_all_comments(user_id)
    df = pd.DataFrame.from_dict(new_com)
    df.to_csv(f"./comments/{user_id}/{user_id}_comments.csv", index=False)


def get_comments(user_id, save_path="./comments"):
    p = pathlib.Path(f"./comments/{user_id}")
    p.mkdir(parents=True, exist_ok=True)
    step = 0
    while True:
        time.sleep(0.2)
        try:
            res = requests.get(f"https://api.dtf.ru/v1.9/user/{user_id}/comments?count=50&offset={step}").content
            fn = f"{user_id}_offset_{step}.json"
            filepath = p / fn
            with open(str(filepath), 'w') as f:
                json.dump(json.loads(res), f)
            if len(json.loads(res)['result']) <50:
                break
            step+=50
        except ConnectionResetError:
            with open("bad_index.txt", 'a') as fp:
                fp.write(f"{user_id}\n")
            print("ConnectionResetError: " ,user_id, step)
            break
        except Exception as e:
            with open("bad_index.txt", 'a') as fp:
                fp.write(f"{user_id}\n")
            print(user_id)
            print(e)
            break
    try:
        convert_to_csv(user_id)
        clear(user_id)
    except Exception as e:
        print(e)
        with open("bad_index.txt", 'a') as fp:
                fp.write(f"{user_id}\n")


if __name__ == '__main__':
    p = pathlib.Path(f"./comments/")
    p.mkdir(parents=True, exist_ok=True)
    process_map(get_comments, list(range(80500, 100500)), max_workers=32, chunksize=1)
