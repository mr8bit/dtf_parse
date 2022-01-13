import pathlib
from argparse import ArgumentParser

import pandas as pd
from tqdm import tqdm

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--start", type=int, required=True)
    parser.add_argument("-e", "--end", type=int, required=True)
    args = parser.parse_args()

    print("=" * 20)
    print("Настройки комплементации..")
    print(f"Начальный индекс парсинга: {args.start}")
    print(f"Конченый индекс парсинга: {args.end}")
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
    df.to_csv(f"./complimentation_{args.start}_{args.end}.csv")
