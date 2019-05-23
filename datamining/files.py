import os
import pandas as pd


DEFAULT_PATH = {
    "posts": "./data/posts/",
    "likes": "./data/likes",
}


def merge_csv(path, verbose=False):
    """Merge all csv tables (csv batches) from path into one pandas.DataFrame"""
    table = []

    for file_name in next(os.walk(path))[2]:
        next_table = pd.read_csv(os.path.join(path, file_name), sep=";", engine="python", encoding="utf-8")
        next_table["by_tag"] = file_name.split("_")[0]
        if verbose:
            print(f"Reading table: {file_name}")
        table.append(next_table)

    return pd.concat(table, sort=False)
