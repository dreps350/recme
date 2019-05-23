"""
This module instead of likes_mining.py was written
before I have found 'instagram' library, so it is hardcoded a little
"""


import json
import time
import argparse
import os

import requests
import pandas as pd

from datamining.files import DEFAULT_PATH

GLOBAL_VERBOSE = True
GLOBAL_KEYS = [
    "text",
    "taken_at_timestamp",
    "edge_liked_by",
    "owner",
    "is_video"
]


def get_json(path, next_page=None, retries=5, verbose=False):
    """
    Returns json response from url https://www.instagram.com/ + path
    :param path: part of url that follows https://www.instagram.com/
    :param next_page: link to next page similar to linked list, got from 'cursor' field in response json
    :param retries: number of retries to get response from url before skip for next
    :param verbose: if true errors information will be printed in stdout
    :return: json response as dictionary
    """

    url = "https://www.instagram.com/" + path
    params = {
        "__a": 1
    }
    retries_counter = 0

    if next_page:
        params["max_id"] = {next_page}

    while retries_counter <= retries:
        try:
            r = requests.get(url, params=params)
            retries_counter = 0
            return r.json()
        except OSError as e:
            retries_counter += 1
            sleep_for = retries_counter * 10
            if verbose:
                print(e, f"\nSleeping for {sleep_for} seconds...")
            time.sleep(sleep_for)
        except json.decoder.JSONDecodeError as e:
            retries_counter += 1
            sleep_for = retries_counter * 10
            if verbose:
                print(e, f"\nSleeping for {sleep_for} seconds...")
            time.sleep(sleep_for)
    return {}


def pages_by_tag(tag, max_pages, retries=5):
    """Instagram pages generator, yields n pages found by hash tag, where n <= max_pages

    tag: str, instagram hash tag without '#' symbol
    max_pages: int, max n of pages to yield
    retries: int, number of attempts to get data from page

    yields: dict"""

    next_page = None
    page_n = 1
    path = f"explore/tags/{tag}/"
    retries_counter = 0

    while page_n <= max_pages:
        data = get_json(path, next_page)
        if page_n == 1 or page_n % 10 == 0:
            print(f"Got page {page_n} from tag {tag}")

        try:
            page_info = data["graphql"]["hashtag"]["edge_hashtag_to_media"]["page_info"]
            retries_counter = 0
        except KeyError:
            if retries_counter <= retries:
                time.sleep(1)
                print(f"Attempt #{retries_counter} on page {page_n}...")
                retries_counter += 1
                time.sleep(retries_counter * 10)
                continue
            else:
                print(f"Unable to get page data\ncursor: {next_page}")
                break  # is not really useful

        # checking link for next page
        if page_info["has_next_page"]:
            next_page = page_info["end_cursor"]
        else:
            print(f"Got {page_n} total pages from tag {tag}")
            yield data  # fixed for single page total
            break

        page_n += 1

        yield data
    

def parse_page(page_json, keys, verbose=False):
    """Parses instagram page json
    page_json: dict, json from server response
    keys: iterable, list of fields to retrieve from page_json

    returns: dict"""

    parsed_data = {}
    page_posts = page_json["graphql"]["hashtag"]["edge_hashtag_to_media"]["edges"]
    for post in page_posts:
        raw_post = post["node"]
        parsed_post = {}
        for key in keys:
            try:
                parsed_post[key] = raw_post[key]
            except KeyError:
                try:
                    parsed_post[key] = raw_post["edge_media_to_caption"]["edges"][0]["node"][key]
                except KeyError:
                    if verbose:
                        print(f"Key {key} was not found")
                except IndexError as e:
                    if verbose:
                        print(f"Key {key} was not found:\n{e}")
        parsed_data[raw_post["shortcode"]] = parsed_post
    return parsed_data


def get_tag_data(tag, path, max_pages=300, dump=False, prefix=None, rewrite=False):
    """Collects data from instagram posts found by tag

    tag: str, instagram hash tag without '#' symbol
    keys: fields from instagram response json to retrieve
    max_pages: int, max n of pages to parse

    returns: dict, with pairs <post_shortcode>: {<post_data>}"""

    if prefix is not None:
        file_name = os.path.join(path, f"{prefix}.csv")
    else:
        file_name = os.path.join(path, f"{tag}.csv")

    if dump and not rewrite and os.path.exists(file_name):
        return

    tag_data = {}
    for page in pages_by_tag(tag, max_pages):
        time.sleep(2)
        page_data = parse_page(page, GLOBAL_KEYS)
        tag_data.update(page_data)

    if dump:
        json_to_df(tag_data, tag).to_csv(file_name, sep=";", index=False)
    else:
        return tag_data


def json_to_df(json_data, tag):
    """Converts dict like data to pandas.DataFrame"""
    columns = {
        "index": "post_id",
        "taken_at_timestamp": "date",
        "edge_liked_by": "likes",
        "owner": "owner_id"
    }
    df = pd.DataFrame.from_dict(json_data, orient="index").reset_index().rename(columns=columns)
    df.likes = df.likes.map(lambda x: x['count'])
    df.owner_id = df.owner_id.map(lambda x: x['id'])
    df["by_tag"] = tag
    return df


def get_all_posts(tags, path=None, max_pages=300, rewrite=False):
    """
    Dump data searched by instagram tags to path
    :param tags: list of instagram hash tags or path to csv file with tags
    :param path: path to store obtained data, if None stored into './data/posts'
    :param max_pages: max number of instagram search pages to process
    :param rewrite: if True files with same names would be rewrited
    """

    path = path or DEFAULT_PATH["posts"]

    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            print(f"Unable to access directory:\n{path}")
            raise e

    for index, tag in enumerate(tags):
        prefix = f"{tag}_{index}"
        print(f"Collecting data {index + 1} / {len(tags)} for tag {tag}")
        get_tag_data(tag, dump=True, path=path, prefix=prefix, max_pages=max_pages, rewrite=rewrite)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""Collects posts data found by hash tags 
                                                    and stores it in path""")
    parser.add_argument('tags', help="Path to instagram hash tag csv without '#' symbol")
    parser.add_argument('path', default=DEFAULT_PATH["posts"], help="Path to store obtained data")
    parser.add_argument('-p', '--pages', default=300, type=int, help="Max number of pages to process")
    args = parser.parse_args()

    # tag_data = get_tag_data(args.tag, max_pages=args.pages)
    #
    # path = os.path.join(args.path, f"{args.tag}.json")
    # with open(path, "w") as f:
    #     json.dump(tag_data, f)

    tags = pd.read_csv(args.tags, sep=";", header=None)[1].values

    get_all_posts(tags, path=args.path, max_pages=args.pages)
