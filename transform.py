import json
import os
import pathlib
from argparse import ArgumentParser

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


def transform_and_clear(user_id):
    try:
        convert_to_csv(user_id)
        clear(user_id)
    except:
        with open("bad_index.txt", 'a') as fp:
            fp.write(f"{user_id}\n")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    parser.add_argument("-w", "--workers", type=int, default=32)
    args = parser.parse_args()

    process_map(transform_and_clear, list(range(args.start, args.end)), max_workers=args.workers, chunksize=1)

    print("=" * 20)
    print("Настройки трансформа")
    print(f"Начальный индекс трансформа: {args.start}")
    print(f"Конченый индекс трансформа: {args.end}")
    print(f"Количество воркеров: {args.workers}")
    print("=" * 20)

    print("Начата комплементация..")
    p = pathlib.Path(f"./comments")
    comm_file = p.glob("*/*.csv")
    print("Сбор всех файлов...")
    concat = []
    for file in tqdm(comm_file):
        try:
            concat.append(pd.read_csv(file, index_col="id"))
        except pd.errors.EmptyDataError:
            pass
        except pd.errors.ParserError:
            pass
        except TypeError:
            print(file)
    print("Комплементация...")
    df = pd.concat(concat)
    print("Завершена!")
    df.to_csv(f"./complimentation_{args.start}_{args.end}_transformed.csv")
    print(f"Файл complimentation_{args.start}_{args.end}_transformed.csv")
    print("Готов")
