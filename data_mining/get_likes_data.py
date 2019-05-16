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
AG = WebAgent()


def get_post_likes(shortcode: str):
    """
    Collects likes from instagram post by shortcode
    """

    media = Media(shortcode)
    pointer = None
    for i in range(3):
        time.sleep(random.random() * 2)
        try:
            likes, pointer = AG.get_likes(media, pointer)
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
def get_all_likes(posts, path, start=0, stop=None, batchsize=500):
    """
    Collects likes data for given post shortcodes and stores it in format "likes_{batch_start}_{batch_stop}.csv"
    :param posts: iterable, posts shortcodes
    :param path: str, path to folder, wich will contain resulr data csv
    :param start: index of posts to start iterate over
    :param stop: index + 1 to stop iterate, if None - posts len will be used
    :param batchsize: number of posts wich data will be stored in separate csv file
    :return: None
    """

    stop = stop or len(posts)
    likes = []
    start_time = time.time()
    batch_start, batch_stop = start, start + batchsize

    # iterate over posts
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
            likes_df.to_csv(os.path.join(path, f"likes_{batch_start}_{batch_stop}.csv"), sep=";", index_label="post_id")

            batch_start, batch_stop = i, i + batchsize
            likes = []

    # Adding last batch
    if likes:
        likes_df = pd.concat(likes, sort=False)
        likes_df.to_csv(os.path.join(path, f"likes_{batch_start}_{stop}.csv"), sep=";", index_label="post_id")

    print(f"Finished after {time.time() - start_time}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""Collects users who liked the post by post's shortcode
                                                    \nand dumps data to 'path' folder""")
    parser.add_argument('-p', '--path', default=STORE_PATH, help="Path to store obtained data")
    parser.add_argument('-b', '--batch', default=200, type=int, help="Max number posts to dump in single file")
    parser.add_argument('--start', default=0, type=int, help="Start index in posts shortcodes array")
    parser.add_argument('--stop', default=None, type=int, help="Stop index in posts shortcodes array")
    args = parser.parse_args()

    # TODO create folder anyway, without a flag
    if not os.path.exists(STORE_PATH):
        try:
            os.makedirs(STORE_PATH)
        except OSError as e:
            print(f"Unable to access directory:\n{STORE_PATH}")
            raise e

    # Read csv data, collected by get_tag_data.py
    data = pd.read_csv(DATA_PATH, sep=";")
    posts = data[~data["is_video"]]["post_id"].values

    get_all_likes(posts, args.path, start=args.start, stop=args.stop, dumpn=args.batch)
