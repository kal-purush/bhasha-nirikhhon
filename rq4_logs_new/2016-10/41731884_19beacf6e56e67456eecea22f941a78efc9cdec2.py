"""
.. module:: SOM

SOM
*************

:Description: SOM

    

:Authors: bejar
    

:Version: 

:Created on: 29/09/2016 9:07 

"""


from sklearn import datasets
from amltlearn.preprocessing import Discretizer
from pylab import *
import seaborn as sns
import matplotlib.pyplot as plt
import somoclu

__author__ = 'bejar'


iris = datasets.load_iris()

som = somoclu.Somoclu(2, 3, data=iris['data'])
som.train(epochs=100)
from sklearn.cluster import KMeans
som.view_umatrix(bestmatches=True, bestmatchcolors=iris['target'])

km = KMeans(n_clusters=3)
som.cluster(algorithm=km)
print(som.bmus)
for x, y in som.bmus:
    print(som.clusters[x,y])

som.view_umatrix(bestmatches=True)