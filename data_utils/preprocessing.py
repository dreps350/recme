import re
from collections import Counter

import numpy as np
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

from pymorphy2 import MorphAnalyzer
from nltk.corpus import stopwords

from data_utils.data_helper import FreqCounter


CITIES = pd.read_csv("./data/cities_.csv", sep=";", header=None)[1].values
SPECIAL_WORDS = pd.read_csv("./data/special_words.csv", sep=";", header=None)[1].values

my_stop_words = {
    "мастеркласс",
    "мастер",
    "класс",
    "мк",
}
STOPWORDS = set(stopwords.words("russian")).union(my_stop_words)


# TODO: write some docs

class FeatureExtractor(FreqCounter):
    """
    Creates new pd.DataFrame with some feature column.
    """

    @staticmethod
    def _check_fields(df, fields):
        for f in fields:
            if f not in df:
                raise ValueError(f"DataFrame must contain {f} field")

    def _add_contacts(self, df: "pd.DataFrame"):
        """Adds phone numbers and bollean direct features"""
        self._check_fields(df, ["text"])
        df = df.copy()
        # Russian mobile phone | instagram direct pattern
        contacts_pat = re.compile(
            r"([\+7|7|8]?[\s\-]?\(?[489][0-9]{2}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2})|(директ)")
        df[["phone_number", "direct"]] = df["text"].str.extract(contacts_pat)
        return df

    def _add_price(self, df: "pd.DataFrame"):
        """Adds price feature"""
        self._check_fields(df, ["text"])
        df = df.copy()
        price_pat = re.compile(r"([\d]+0)\s?[р]")  # "420 р" and "4000р" are prices, "421" is not
        df["price"] = (df["text"].str.lower()
                       .str.extract(price_pat)
                       .fillna(0)
                       .iloc[:, 0]  # convert to Series
                       .map(lambda x: 0 if isinstance(x, str) and len(x) > 5 else x)
                       .astype("int64"))
        return df

    def filter_workshops(self, df: "pd.DataFrame"):
        """Remove all rows that are not workshops"""
        df = df.copy()
        df = (df.pipe(self._add_contacts)
                .pipe(self._add_price))

        contact_filter = df[["phone_number", "direct"]].notnull().any(axis=1)
        price_filter = df["price"].between(500, 10000)

        return df[contact_filter & price_filter]

    def _add_city(self, df: "pd.DataFrame"):
        pass

    def _add_tags(self, df: "pd.DataFrame", pattern="any_tag", key_words=None):
        self._check_fields(df, ["text"])
        df = df.copy()
        self.fit(df["text"], pattern, key_words)
        df["tags"] = self._tags
        df.apply(lambda x: x["tags"].add(x["by_tag"]), axis=1)  # to fill empty instances

        return df

    def _add_stripped_tags(self, df: "pd.DataFrame", key_words):
        self._prepare_pattern("strip_kw", key_words)
        df = df.copy()

        def strip_tags(tags):
            res = []
            for t in tags:
                t = re.sub(self._pattern, "", t)
                if t:
                    res.append(t)
            return res

        df["stripped_tags"] = df["tags"].map(lambda x: strip_tags(x))

        return df


def memo(func):
    """
    Memoize function f, whose args must all be hashable.
    """
    cache = {}

    def wrapped(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]
    return wrapped


class NoSpaceSplitter:

    def __init__(self, counter: "Counter"):
        """
        Utility to recover the sequence of words from
        given sequence of characters with no spaces separating words.
        :param counter: collections.Counter object with words frequencies in text corpus
        """
        self.p = self.pdist(counter)

    @staticmethod
    def pdist(counter):
        """
        Make a probability distribution, given evidence from a Counter.
        """
        n = sum(counter.values())
        return lambda x: counter[x] / n

    def pwords(self, words):
        """
        Probability of words, assuming each word is independent of others.
        """
        return np.prod([self.p(w) for w in words])

    @staticmethod
    def splits(text, start=0, max_len=20):
        """
        Return a list of all (first, rest) pairs; start <= len(first) <= L.
        """
        return [(text[:i], text[i:])
                for i in range(start, min(len(text), max_len) + 1)]

    @memo
    def segment(self, text):
        """
        Return a list of words that is the most probable segmentation of text.
        """
        if not text:
            return []

        candidates = [[first] + self.segment(rest)
                      for (first, rest) in self.splits(text, 1)]
        res = sorted(candidates, key=self.pwords, reverse=True)

        return res[0]


class TextPreprocessor:

    def __init__(self, mode="normal", token="[а-я]+", counter=None, allowed_pos=None):
        if mode not in {"normal", "nospace"}:
            raise ValueError("Unknown mode.")
        self.mode = mode
        self.token = token
        self.allowed_pos = allowed_pos
        self.morph = MorphAnalyzer()
        if mode == "nospace":
            self.nospace = NoSpaceSplitter(counter)

    def _tokenize(self, doc):
        if callable(self.token):
            return self.token(doc)
        return re.findall(self.token, doc)

    def _clear_stop_words(self, doc):
        return [w for w in doc if w not in STOPWORDS]

    def _lemmatize(self, doc):
        res = []
        for w in doc:
            parsed = self.morph.parse(w)[0]
            if self.allowed_pos:
                if parsed.tag.POS in self.allowed_pos:
                    res.append(parsed.normal_form)
                else:
                    continue
            else:
                res.append(parsed.normal_form)
        return res

    def _no_space_split(self, doc):
        res = []
        for w in doc:
            split = self.nospace.segment(w)
            if len(split) <= 2:
                res.extend(split)
        return res

    def _preprocess(self, corpora):
        data = (corpora.str.lower()
                .map(self._tokenize)
                .map(self._lemmatize)
                .map(self._clear_stop_words))
        return data

    def transform(self, corpora: "pd.Series"):
        if self.mode == "normal":
            return self._preprocess(corpora)
        elif self.mode == "nospace":
            return self._preprocess(corpora).map(self._no_space_split)