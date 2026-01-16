import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans

df = pd.read_csv("SemesterProjectStats.csv")

#wOBACON / xwOBACON Cluster Comparison

xwoba = df[['wobacon', 'xwobacon']]
kmeans = KMeans(n_clusters=2)
kmeans.fit(xwoba)
labels = kmeans.labels_
centroids = kmeans.cluster_centers_

#Plotting
for cluster in range(2):  
    cluster_points = xwoba[labels == cluster]
    plt.scatter(cluster_points['wobacon'], cluster_points['xwobacon'], label=f"Cluster {cluster}")

plt.scatter(centroids[:, 0], centroids[:, 1], s=100, c='red', marker='X', label='Centroids')
plt.title('wOBACON / xwOBACON')
plt.legend()
plt.show()

#Blasts Contact / Blasts Swings Cluster Comparison

blast = df[['blasts_contact', 'blasts_swing']]
kmeans = KMeans(n_clusters=2)
kmeans.fit(blast)
labels = kmeans.labels_
centroids = kmeans.cluster_centers_

#Plotting
for cluster in range(2):  
    cluster_points = blast[labels == cluster]
    plt.scatter(cluster_points['blasts_contact'], cluster_points['blasts_swing'], label=f"Cluster {cluster}")

plt.scatter(centroids[:, 0], centroids[:, 1], s=100, c='red', marker='X', label='Centroids')
plt.title('Blasts/Contact | Blasts/Swings')
plt.legend()
plt.show()