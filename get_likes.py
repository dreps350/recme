import os
import time
import random
import argparse

import pandas as pd
from instagram.entities import Media
from instagram.agents import WebAgent
from instagram.exceptions import InternetException
from requests.exceptions import ConnectionError


STORE_PATH = "./data/likes"
DATA_PATH = "./data/main_data.csv"


def get_post_likes(shortcode: str):
    """
    Collects likes from instagram post by shortcode
    """

    media = Media(shortcode)
    ag = WebAgent()
    pointer = None
    for i in range(3):
        time.sleep(random.random() * 2)
        try:
            likes, pointer = ag.get_likes(media, pointer)
            # if pointer is None:
            #     return media.likes
        except (ConnectionError, InternetException) as e:
            delay = random.randint(20, 100)
            if not ("404" in repr(e)):
                print(e, f"Sleep for {delay} sec...")
                time.sleep(delay)
    return media.likes


# TODO: Dump obtained data on every n instance
# TODO: Add restart from some index ability
def get_all_likes(posts, path, start=0, stop=None, dumpn=500):
    stop = stop or len(posts)
    likes = []
    start_time = time.time()
    batch_start, batch_stop = start, start + dumpn

    for i, p in enumerate(posts[start:stop], start=start):
        print(f"Collecting likes from {p} #{i}")
        try:
            gathered_likes = get_post_likes(p)
            if gathered_likes:
                post_df = pd.DataFrame({user: 1 for user in gathered_likes}, index=[p])
                likes.append(post_df)
                print("Done")
            else:
                print("Fail")
        except Exception as e:
            print(e)
            continue

        if i == batch_stop:
            likes_df = pd.concat(likes, sort=False)
            likes_df.to_csv(os.path.join(path, f"likes_{batch_start}_{batch_stop}.csv"), sep=";", index=False)

            batch_start, batch_stop = i, i + dumpn
            likes = []

    # Adding last batch
    if likes:
        likes_df = pd.concat(likes, sort=False)
        likes_df.to_csv(os.path.join(path, f"likes_{batch_start}_{stop}.csv"), sep=";", index=False)

    print(f"Finished after {time.time() - start_time}")


# TODO: add argparser
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""Collects users who liked the post by post's shortcode
                                                    \nand dumps data to 'path' folder""")
    parser.add_argument('-p', '--path', default=STORE_PATH, help="Path to store obtained data")
    parser.add_argument('-b', '--batch', default=200, type=int, help="Max number posts to dump in single file")
    parser.add_argument('--start', default=0, type=int, help="Start index in posts shortcodes array")
    parser.add_argument('--stop', default=None, type=int, help="Stop index in posts shortcodes array")
    parser.add_argument('-d', '--mkdir', default=False, type=bool, help="Pass 1 to create data dir.")
    args = parser.parse_args()

    if args.mkdir:
        try:
            os.makedirs(STORE_PATH)
        except OSError as e:
            print("Data directory already exists\n", e)

    # Read csv data, collected by get_tag_data.py
    data = pd.read_csv(DATA_PATH, sep=";")
    posts = data[data["is_video"].map(lambda x: not x)]["post_id"].values

    get_all_likes(posts, args.path, start=args.start, stop=args.stop, dumpn=args.batch)
