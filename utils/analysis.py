import re
import os
from collections import Counter

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn import metrics


class KMeansAnalyzer:
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
        Fits clusterer with n clusters according to passed range object.
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
    def __init__(self):
        self._freqs = Counter()
        self._data = pd.DataFrame()

    def fit(self, iterable):
        self._freqs.update([item for sub_item in iterable for item in sub_item])

        self._data = (pd.Series(self._freqs)
                      .sort_values(ascending=False)
                      .reset_index()
                      .rename({"index": "item", 0: "freq"}, axis=1))

        return self

    def data(self, max_df=0.95):
        if self._data:
            return self._data[self._data["freq"] <= self._data["freq"].quantile(max_df)]
        else:
            raise ValueError("Data was not found")

    def top_items(self, n):
        if self._freqs:
            return self._freqs.most_common(n)
        else:
            raise ValueError("Items were not found")
