import pathlib

import pandas as pd
from tqdm import tqdm

if __name__ == '__main__':

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
    df.to_csv("./complimentation.csv")
