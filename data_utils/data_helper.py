import re
import os
from collections import Counter

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn import metrics


class KMeansAnalizer:
    """
    Helps to determine number of clusters according
    to mean squared distance or silhouette metric.
    """

    def __init__(self, clusterer, data, verbose=False):
        """
        :param clusterer: an instance of sklearn.cluster.KMeans or MiniBatchKMeans
        :param data: data array to fit clusterer
        :param verbose: if True will print the current state while executing methods
        """
        self._clusterer = clusterer
        self._data = data
        self._verbose = verbose
        self._inertia = {}
        self._labels = {}
        self._silhouette = {}

    def explore(self, range_inst: "range"):
        """
        Fits clusterer with n clusters.
        :param range_inst: range of clusters to fit
        :return: None
        """
        for k in range_inst:
            if k in self._inertia:
                continue
            if self._verbose:
                print(f"fitting {k} clusters")
            self._clusterer.n_clusters = k
            self._clusterer.init_size = k * 3
            self._clusterer.fit(self._data)
            self._inertia.update({k: np.sqrt(self._clusterer.inertia_)})
            self._labels.update({k: self._clusterer.labels_})

    def plot_inertia(self):
        plt.plot(self._inertia.keys(), self._inertia.values(), marker='s')
        plt.xlabel('$k$')
        plt.ylabel('$J(C_k)$')

    def _calculate_silhouette(self):
        for k, labels in self._labels.items():
            if k not in self._silhouette:
                if self._verbose:
                    print(f"Calculating silhouette for {k} clusters")
                self._silhouette.update({k: metrics.silhouette_score(self._data, labels)})

    def plot_silhouette(self):
        self._calculate_silhouette()
        plt.plot(self._silhouette.keys(), self._silhouette.values(), marker='s')
        plt.xlabel('$k$')
        plt.ylabel('$Silhouette$')


class FreqCounter:
    def __init__(self, n_top_tags=1000):
        self._freqs = Counter()
        self._n_top_tags = n_top_tags
        self._data = pd.DataFrame()
        self._tags = []
        self._pattern = ""

    def _prepare_pattern(self, pattern, key_words=None):
        if pattern == "word":
            self._pattern = re.compile(r"\w+")
        elif pattern == "any_tag":
            self._pattern = re.compile(r"#(\w+)")
        elif pattern == "kw_tag":  # extracting tags with key words
            res = []
            for kw in key_words:
                if "\\" in kw:  # processing start/end only key words
                    kw = kw.strip("\\")
                    res.append(r"\w*{}".format(kw))
                    res.append(r"{}\w*".format(kw))
                else:
                    res.append(r"\w*{}\w*".format(kw))
            tags_pat = "|".join(res)  # result pattern contains logical OR expression with all key words
            self._pattern = re.compile(r"#({})(?=[#\W]|$)".format(tags_pat))
        elif pattern == "strip_kw":
            res = []
            for kw in key_words:
                res.append(kw)
            self._pattern = re.compile("|".join(res))
        else:
            self._pattern = re.compile(pattern)

    def _tokenize(self, iterable):
        for row in iterable:
            yield re.findall(self._pattern, row)

    def fit(self, iterable, pattern="word", key_words=None, tokenize=True):
        self._prepare_pattern(pattern, key_words)
        self._tags.clear()

        if tokenize:
            tokenizer = self._tokenize(iterable)
        else:
            tokenizer = iterable

        for term in tokenizer:
            self._tags.append(set(term))

        self._freqs.update([item for sub_item in self._tags for item in sub_item])

        self._data = (pd.Series(self._freqs)
                      .sort_values(ascending=False)
                      .reset_index()
                      .rename({"index": "tag", 0: "freq"}, axis=1))

        return self

    def data(self, quantile=0.9):
        return self._data[self._data["freq"] > self._data["freq"].quantile(quantile)]

    @property
    def top_tags(self):
        return {key[0] for key in self._freqs.most_common(self._n_top_tags)}


def merge_from_path(path, verbose=False):
    """Merges all csv tables (csv batches) from path into one pandas.DataFrame"""
    table = []

    for file_name in next(os.walk(path))[2]:
        next_table = pd.read_csv(os.path.join(path, file_name), sep=";", index_col=0, engine="python", encoding="utf-8")
        next_table["by_tag"] = file_name.split("_")[0]
        if verbose:
            print(f"Reading table: {file_name}")
        table.append(next_table)

    return pd.concat(table, sort=False)



