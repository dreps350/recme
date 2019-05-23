import os
import time
import random
import argparse
import json

import pandas as pd
from instagram.entities import Media
from instagram.agents import WebAgent
from instagram.exceptions import InternetException
from requests.exceptions import ConnectionError

from datamining.files import DEFAULT_PATH


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


def get_all_likes(posts, path, start=0, stop=None, batch=500):
    """
    Collect likes data for given post shortcodes and stores it in format "likes_{batch_start}_{batch_stop}.csv"
    :param posts: iterable, posts shortcodes
    :param path: str, path to folder, which will contain result data csv
    :param start: index of posts to start iterate over
    :param stop: index + 1 to stop iterate, if None - posts len will be used
    :param batch: number of posts which data will be stored in separate csv file
    """

    stop = stop or len(posts)
    likes = []
    start_time = time.time()
    batch_start, batch_stop = start, start + batch

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

            batch_start, batch_stop = i, i + batch
            likes = []

    # Adding last batch
    if likes:
        likes_df = pd.concat(likes, sort=False)
        likes_df.to_csv(os.path.join(path, f"likes_{batch_start}_{stop}.csv"), sep=";", index_label="post_id")

    print(f"Finished after {time.time() - start_time}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""Collects users who liked the post by post's shortcode
                                                    \nand dumps data to 'path' folder""")
    parser.add_argument('posts', help="""Path to csv file with list of instagram posts stored in 'posts_id' field
                                            or in the only column""")
    parser.add_argument('-p', '--path', default=DEFAULT_PATH["likes"], help="Path to store obtained data")
    parser.add_argument('-b', '--batch', default=200, type=int, help="Max number posts to dump in single file")
    parser.add_argument('--start', default=0, type=int, help="Start index in posts shortcodes array")
    parser.add_argument('--stop', default=None, type=int, help="Stop index in posts shortcodes array")
    args = parser.parse_args()

    if not os.path.exists(DEFAULT_PATH["likes"]):
        try:
            os.makedirs(DEFAULT_PATH["likes"])
        except OSError as e:
            print(f"Unable to access directory:\n{DEFAULT_PATH['likes']}")
            raise e

    posts = pd.read_csv(args.posts, sep=";", header=None)
    if "post_id" in posts:
        posts = posts["post_id"].values
    else:
        posts = posts[1].values

    get_all_likes(posts, args.path, start=args.start, stop=args.stop, batch=args.batch)
