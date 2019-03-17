import json
import time
import argparse

import requests


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
    Returns json from url https://www.instagram.com/ + path

    path: str, part of url that follows https://www.instagram.com/
    next_page: str, link to next page similar to linked list,
    got from 'cursor' field in response json

    returns: dict"""

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
    """Instagram pages generator, yields n pages found by hash tag,
    where n is less or equal than max_pages

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
        print(f"Got page {page_n}")

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

        # checking link to next page
        if page_info["has_next_page"]:
            next_page = page_info["end_cursor"]
        else:
            print("No more pages")
            break

        page_n += 1

        yield data


# можно использовать как декоратор
def parse_page(page_json, keys, verbose=True):
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


def get_tag_data(tag, keys, max_pages=300):
    """Collects data from instagram posts found by tag

    tag: str, instagram hash tag without '#' symbol
    keys: fields from instagram response json to retrieve
    max_pages: int, max n of pages to parse

    returns: dict, with pairs <post_shortcode>: {<post_data>}"""

    tag_data = {}
    for page in pages_by_tag(tag, max_pages):
        time.sleep(2)
        page_data = parse_page(page, keys)
        tag_data.update(page_data)
    return tag_data


if __name__ == '__main__':
    # tag = "мастерклассспб"
    parser = argparse.ArgumentParser(description="""Process Instagram pages found by tag 
                                                    and stores it in ./<tag_name>.json""")
    parser.add_argument('tag', help="Instagram hash tag without '#' symbol")
    parser.add_argument('-p', '--pages', default=300, type=int, help="Max number of pages to process")
    args = parser.parse_args()
    tag_data = get_tag_data(args.tag, GLOBAL_KEYS, args.pages)

    with open(f"{args.tag}.json", "w") as f:
        json.dump(tag_data, f)

