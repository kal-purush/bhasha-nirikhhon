#!/usr/bin/env python
# coding: utf-8

from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.neighbors import KNeighborsClassifier
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import numpy as np

# base de dígitos padão da sklearn
# x: as features/caracteristicas em si
# y: são os rótulos/classes
# x, y = load_digits(return_X_y=True)

# load data
print("Loading data...")
tr = np.loadtxt('treinamento.txt')
y_train = tr[:, 132]
x_train = tr[:, : 132]

tr = np.loadtxt('teste.txt')
y_test = tr[:, 132]
x_test = tr[:, : 132]

# normalização
ss = StandardScaler()
x_train = ss.fit_transform(x_train)
x_test = ss.fit_transform(x_test)


classes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
# classes = [0] # um de cada vez
centroids = []

for classe in classes:

    xc_train = []
    yc_train = []

    xc_test = []
    yc_test = []
    
    idxs = y_train == classe
    xc_train.append(x_train[idxs,:])
    yc_train.extend(y_train[idxs])

    idxs = y_test == classe
    xc_test.append(x_test[idxs,:])
    yc_test.extend(y_test[idxs])
    
    xc_train = np.concatenate(xc_train)
    yc_train = np.array(yc_train)
    
    xc_test = np.concatenate(xc_test)
    yc_test = np.array(yc_test)
        
    # x_train, x_test, y_train, y_test = train_test_split(xc, yc, test_size=0.2)

    # normalização tava aqui

    # a fim de plotar e ver oq ta acontecendo (redução de dimensionalidade para 2 dimensões)
    pca = PCA(n_components=2)
    pca.fit(xc_train)
    x_train_pca = pca.transform(xc_train)
    x_test_pca = pca.transform(xc_test)

    for digit_class in sorted(list(set(yc_train))):
        indexes = yc_train == digit_class
        plt.scatter(x_train_pca[indexes,0], x_train_pca[indexes,1], label=str(digit_class))
    plt.legend()

    km = KMeans(n_clusters=5) # alterar esse parametro: 5, 10, 20
    km.fit(x_train_pca)


    plt.scatter(km.cluster_centers_[:,0], km.cluster_centers_[:,1], s=100, marker='x') # cluster_centers_ contem os k os centroides
    plt.show()

    centroids.append(km.cluster_centers_)

# centroids: treinamento obtido a apartir do K-means para ser usado como treino dos outros classificadores
print(centroids)



# KNN


# DT