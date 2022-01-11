import json
import os
import pathlib
import random
import time

import pandas as pd
import requests
from tqdm.contrib.concurrent import process_map  # or thread_map


def extract(file):
    res = []
    with open(str(file), "r") as read_file:
        data = json.load(read_file)
        for item in data['result']:
            res.append({
                'id': item['id'],
                'user_id': item['author']['id'],
                'date': item['date'],
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


headers = {'User-Agent': 'dtf-app/2.2.0 (Pixel 2; Android/9; ru; 1980x1794'}


def get_comments(user_id, save_path="./comments"):
    p = pathlib.Path(f"./comments/{user_id}")
    p.mkdir(parents=True, exist_ok=True)
    step = 0
    time.sleep(5 + random.randint(0, 50))
    while True:
        try:
            response = requests.get(f"https://api.dtf.ru/v1.9/user/{user_id}/comments?count=50&offset={step}",
                                    headers=headers)
            response.raise_for_status()
            if response.status_code != 204:
                fn = f"{user_id}_offset_{step}.json"
                filepath = p / fn
                with open(str(filepath), 'w') as f:
                    json.dump(response.json(), f)
                if len(response.json()['result']) < 50:
                    break
                else:
                    time.sleep(0.3 + random.randint(0, 15))
                step += 50
        except requests.exceptions.HTTPError:
            time.sleep(1 + random.randint(0, 15))

        except ConnectionResetError:
            with open("bad_index.txt", 'a') as fp:
                fp.write(f"{user_id}\n")
            print("ConnectionResetError: ", user_id, step)
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
        if user_id % 500:
            print(f"Обработан: {user_id}")
    except Exception as e:
        print(e)
        with open("bad_index.txt", 'a') as fp:
            fp.write(f"{user_id}\n")


if __name__ == '__main__':
    print("Начало парсинга...")
    p = pathlib.Path(f"./comments/")
    p.mkdir(parents=True, exist_ok=True)
    process_map(get_comments, list(range(80500, 100500)), max_workers=32, chunksize=1)
    print("Парсинг окончен...")
