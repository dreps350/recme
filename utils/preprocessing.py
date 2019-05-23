import re
import os
from collections import Counter, Iterable

import pandas as pd
import numpy as np

from pymorphy2 import MorphAnalyzer
from nltk.corpus import stopwords


CITIES = set(pd.read_csv("./data/cities_.csv", sep=";", header=None)[1].values)
SPECIAL_WORDS = set(pd.read_csv("./data/special_words.csv", index_col=0, header=None)[1].values)
STOPWORDS = set(stopwords.words("russian"))


class FeatureExtractor:
    """
    Create new pd.DataFrame with some feature column.
    """

    @staticmethod
    def _check_fields(df, fields):
        for f in fields:
            if f not in df:
                raise ValueError(f"DataFrame must contain {f} column")

    def drop_duplicates(self, df: pd.DataFrame):
        self._check_fields(df, ["text"])
        df = df.copy()

        return (df.assign(tokens=df["text"].map(TextProcessing().tokenize).str.join(""))
                  .drop_duplicates("tokens")
                  .drop("tokens", axis=1))

    def add_contacts(self, df: "pd.DataFrame"):
        """Add phone number and bollean direct features"""

        self._check_fields(df, ["text"])
        df = df.copy()

        # Russian mobile phone | instagram direct pattern
        contacts_pat = re.compile(
            r"([\+7|7|8]?[\s\-.]?\(?[489][0-9]{2}\)?[\s\-.]?[0-9]{3}[\s\-.]?[0-9]{2}[\s\-.]?[0-9]{2})|(директ|Директ|direct)"
        )
        df[["phone_number", "direct"]] = df["text"].str.extract(contacts_pat)

        return df

    def add_price(self, df: "pd.DataFrame"):
        """Add price feature"""

        self._check_fields(df, ["text"])
        df = df.copy()

        price_pat = re.compile(r"([\d]+0)\s?[рР₽]")  # "420 р" and "4000р" are prices, "421" is not
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
        df = (df.pipe(self.add_contacts)
                .pipe(self.add_price))

        contact_filter = df[["phone_number", "direct"]].notnull().any(axis=1)
        price_filter = df["price"] > 0

        return df[contact_filter or price_filter]

    def add_tags(self, df: "pd.DataFrame"):
        """Add tag feature with instagram hash tags"""

        self._check_fields(df, ["text"])
        df = df.copy()

        tokenizer = TextProcessing(token_pat="#([a-zа-я_]+)").tokenize
        df["tags"] = (df["text"].map(tokenizer)
                      .map(set))

        df.apply(lambda x: x["tags"].add(x["by_tag"]), axis=1)  # fill if tag was not found
        return df

    def add_split_tags(self, df: "pd.DataFrame", counter):
        pass

    def add_city(self, df: "pd.DataFrame"):
        pass


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
        return max(candidates, key=self.pwords)


class TextProcessing:  # TODO: add stemming
    """
        Make usual text processing such as tokenizing, lemmatizing, deleting stopwords.
        :param token_pat: regex pattern to split text
        :param mode: 'normal' mode provides usual tokenize process, 'nospace' mode is probability based mode to recover
        words from given sequence of characters, requires counter attribute to be passed
        :param counter: Counter object with words frequencies
        :param threshold: max number of divisions in 'nospace' mode to be considered successful
        :param allowed_pos: iterable, parts of speech to be left by pymorphy2.MorphAnalizer after lemmatizing,
        others will be dropped
        :param stop_words: if None is passed default russian stopwords are used
        :param stop_cities: if True russian cities will be dropped
    """

    def __init__(self, token_pat="[а-я]+", mode="normal", counter=None, threshold=3,
                 allowed_pos=None, stop_words=None, stop_cities=False):
        self.token = token_pat
        self.mode = mode

        if self.mode not in {"normal", "nospace"}:
            raise ValueError("Unknown mode")
        elif self.mode == "nospace":
            if not isinstance(counter, Counter):
                raise ValueError("In 'nospace' mode the counter attribute should be passed")
            self.counter = counter
            self.nospace = NoSpaceSplitter(counter)
            self.threshold = threshold

        self.morph = MorphAnalyzer()
        self.allowed_pos = allowed_pos
        self.stop_words = stop_words or STOPWORDS
        if stop_cities:
            self.stop_words.union(CITIES)

    def tokenize(self, doc):
        """
        :param doc: must be a string or iterable, if string it will be splitted in tokens,
        else - left without changes
        :return: list of tokens
        """
        if isinstance(doc, str):
            doc = re.findall(self.token, doc.lower())
        elif not isinstance(doc, Iterable):
            raise ValueError("The doc must be a string or iterable")
        if self.mode == "nospace":
            return self._no_space_split(doc)
        return doc

    def _no_space_split(self, doc):
        res = []
        for w in doc:
            split = self.nospace.segment(w)
            if len(split) <= self.threshold:
                res.extend(split)
            else:
                res.extend(w)
        return res

    def lemmatize(self, doc):
        """
        :param doc: iterable, list of words
        :return: most probable normal forms of words in doc
        """
        res = []
        for w in doc:
            parsed = self.morph.parse(w)[0]
            if parsed in SPECIAL_WORDS:
                continue
            if self.allowed_pos:
                if parsed.tag.POS in self.allowed_pos:
                    res.append(parsed.normal_form)
                else:
                    continue
            else:
                res.append(parsed.normal_form)
        return res

    def clear_stop_words(self, doc):
        """
        :param doc: iterable, list of words
        :return: doc without stopwords
        """
        return [w for w in doc if w not in self.stop_words]

    def transform(self, corpora: "pd.Series"):
        """
        Process full pipeline: tokenizing, deleting stopwords, lemmatizing
        :param corpora: pd.Series to process
        :return: processed data
        """
        data = (corpora.map(self.tokenize)
                .map(self.clear_stop_words)
                .map(self.lemmatize))
        return data
