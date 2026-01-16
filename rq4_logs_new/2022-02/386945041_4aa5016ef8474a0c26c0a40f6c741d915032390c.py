# Import libraries
'''Main'''
import numpy as np
import pandas as pd
import os, time
import pickle, gzip



'''Data Viz'''
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
color = sns.color_palette()

'''Data Prep and Model Evaluation'''
from sklearn import preprocessing as pp
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_curve, average_precision_score
from sklearn.metrics import roc_curve, auc, roc_auc_score
from sklearn.preprocessing import StandardScaler

'''Algorithms'''
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from scipy.cluster.hierarchy import dendrogram, cophenet, fcluster
from scipy.spatial.distance import pdist

import DCTCalc

def main():
    #load data
    data = DCTCalc.batch_calculate_dct(50)
    flattened = DCTCalc.flatten_all(data[0])
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(flattened)

    kmeans = KMeans(
        init="random",
        n_clusters=2,
        n_init=100,
        max_iter=500,
    )

    kmeans.fit(scaled_data)

    print(kmeans.labels_)

if __name__ == "__main__":
    main()