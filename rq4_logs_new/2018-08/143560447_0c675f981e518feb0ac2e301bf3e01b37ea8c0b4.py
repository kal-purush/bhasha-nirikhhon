# =============================================================
# CS5010 Final Project
# Team Avocado
# Jiangxue Han(jh6rg), Sijia Tang(st4je), Rakesh Ravi K U(rk9cx)
# Sakshi Jawarani(sj8em), Elena Gillis(emg3sc)
# =============================================================


from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os

os.chdir('/Users/chloe/Desktop/UVa/Courses/CS5010/Project/dataset/clean_data')
df = pd.read_csv('ol_clean_data.csv')  # Reads in raw data

df = df.dropna() # Drop missing value

# It is not normal to have the zip code not beginning with '100', 
# therefore we fill them with the median of zip code
for i in range(0,len(df)):
    df.loc[df['Zip code'] < 10000, 'Zip code'] = df['Zip code'].median()
    df.loc[df['Location'] > 10, 'Location'] = df['Location'].median()
    
df = df.rename(index=str, columns={"Overall score": "Score", "Number of reviewers": 'numRev'})

## Prepare for k means
df_kmeans = df[['Cleanliness','Comfort','Facilities','Staff','Value for money','Free WiFi']]

df_kmeans = df_kmeans.reset_index(drop=True)  # Resets index for later merge
#data2 = pd.read_csv('data.csv')  # Tong's dataset



headers = list(df_kmeans)
x = []
for header in headers:
    x.append(df_kmeans[header])
x = np.array(x)
x = x.transpose()

# =============================== elbow =======================================
sse = {}
for k in range(1, 10):
    kmeans = KMeans(n_clusters=k, max_iter=1000).fit(x)
#    df_kmeans["clusters"] = kmeans.labels_
    #print(data["clusters"])
    sse[k] = kmeans.inertia_ # Inertia: Sum of distances of samples to their closest cluster center
plt.figure()
plt.plot(list(sse.keys()), list(sse.values()))
plt.xlabel("Number of cluster")
plt.ylabel("SSE")
plt.show()

for n_cluster in range(2, 11):
    kmeans = KMeans(n_clusters=n_cluster).fit(x)
    label = kmeans.labels_
    sil_coeff = silhouette_score(x, label, metric='euclidean')
    print("For n_clusters={}, The Silhouette Coefficient is {}".format(n_cluster, sil_coeff))

# Choose the best K
kmeans = KMeans(n_clusters=3, max_iter=1000).fit(x)
#print (kmeans.labels_)

unique, counts = np.unique(kmeans.labels_, return_counts=True)
dict(zip(unique, counts))  # Counts labels and its times

df_label = pd.DataFrame(data=kmeans.labels_)
print (df_kmeans.shape, df_label.shape)
df_withLabel = pd.concat([df_kmeans,df_label],axis=1)
colNames = list(df_kmeans.columns)
colNames.append('Label')
df_withLabel.columns = colNames


data = df_withLabel.values[:,0:6]
category = df_withLabel.values[:,6]
n = df_withLabel.shape[0]
centers = kmeans.cluster_centers_
colors=['orange', 'blue', 'green']
for i in range(n):
    plt.scatter(data[i, 0], data[i,1], s=7, color = colors[int(category[i])])
plt.scatter(centers[:,0], centers[:,1], marker='*', c=colors, s=150)
centers

# ========================== Add  Label to Data Frame =========================
df2 = pd.merge(df,df_withLabel, how = 'inner', on = ['Cleanliness','Comfort','Facilities','Staff','Value for money','Free WiFi'])
df2 = df2.drop_duplicates()
df2.to_csv('ol_all_newLabel.csv',index=False)


# =================================  3D plot ==================================
from mpl_toolkits.mplot3d import Axes3D
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
x = np.array(df_withLabel['Comfort'])
y = np.array(df_withLabel['Value for money'])
z = np.array(df_withLabel['Free WiFi'])

ax.scatter(x,y,z, marker="s", c=df_withLabel["Label"], s=5, cmap="RdYlBu")

ax.set_xlabel('Comfort')
ax.set_ylabel('Value for money')
ax.set_zlabel('Free WiFi')

plt.show()

# ====================Test Package ============================================
from kmeansPackage import *
elbow(df_kmeans)
kmeans_clustering(df_kmeans,3,2,0,4)