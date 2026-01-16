import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import datetime as dt 
from sklearn.cluster import KMeans
import preprocessing as pp 
import pickle

def cluster(dataframe, n_cluster):
    """
    The function performes kmeans clustering on the input dataframe.
    It also plots graphs to show clustering on a 2d figure(only first 2 dimensions are used for displaying, but all for clustering)

    Arguments:
        dataframe - input dataframe to performe clustering on 
        n_cluster - number of clusters to use, for this task I usually use 2: ok - doesn't look like ok
    Returns:
        classes_df - dataframe, containing class label for each datapoint and coresponding datapoint itself, so its length is equal to the number of datapoint in input dataframe
    """

    kmeans = KMeans(n_clusters=n_cluster)
    kmeans.fit(dataframe)
    c = kmeans.fit_predict(dataframe)
    classes = pd.DataFrame(data=c, columns=["class"], index=dataframe.index.values)
    classes_df = pd.concat([dataframe, classes], axis=1)
    #export kmeans model for a future use 
    pickle.dump(kmeans, open("kmeans_model.sav", 'wb'))

    plt.scatter(dataframe[dataframe.columns.values[0]], dataframe[dataframe.columns.values[1]], c=classes_df['class'])
    plt.show()
    return classes_df