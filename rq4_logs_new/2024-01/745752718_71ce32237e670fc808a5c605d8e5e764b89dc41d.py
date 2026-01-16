import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering, SpectralClustering
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
from scipy.stats import pearsonr
from scipy.cluster import hierarchy
import plotly as py
import plotly.graph_objs as go
from yellowbrick.cluster import KElbowVisualizer
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering, SpectralClustering
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
import warnings
from yellowbrick.cluster import KElbowVisualizer

# Suppress all warnings
warnings.filterwarnings("ignore")

# Load Data
customers = pd.read_csv('mall_customers.csv')

# Basic Information and Descriptive Statistics
print(customers.info())
print(customers.describe())

# Null and Duplicate Values Check
print('Null Values In Dataset: ', customers.isnull().sum())
print('Duplicate Values In Dataset: ', customers.duplicated().sum())

# EDA Visualizations

# Set the aesthetic style of the plots
sns.set_style("whitegrid")

# Custom Color Palette
color_palette = sns.color_palette("pastel")

# Save figures directory (you may need to adjust the path)
save_dir = 'figures'

# 1. Age Distribution with KDE (Kernel Density Estimate)
plt.figure(figsize=(8, 4))
sns.histplot(customers['Age'], bins=30, kde=True, color=color_palette[3])
plt.title('Age Distribution of Customers')
plt.xlabel('Age')
plt.ylabel('Frequency')
plt.savefig('Age_Distribution.png')
plt.close()

# 2. Gender Proportion Pie Chart
gender_counts = customers['Gender'].value_counts()
colors = [color_palette[4], color_palette[0]]
plt.figure(figsize=(6, 6))
plt.pie(gender_counts, labels=gender_counts.index, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title('Gender Distribution')
plt.savefig('Gender_Distribution.png')
plt.close()

# 3. Boxplots for Spending Score by Gender
plt.figure(figsize=(8, 5))
sns.boxplot(x='Gender', y='Spending Score (1-100)', data=customers, palette=[color_palette[1], color_palette[2]])
plt.title('Spending Score by Gender')
plt.savefig('Spending_Score_by_Gender.png')
plt.close()

# 4. Annual Income vs Spending Score Colored by Age
plt.figure(figsize=(10, 6))
sns.scatterplot(x='Annual Income (k$)', y='Spending Score (1-100)', data=customers, hue='Age', palette='cool', legend='brief')
plt.title('Annual Income vs Spending Score Colored by Age')
plt.savefig('Income_vs_Spending_Score.png')
plt.close()

# 5. Count Plot of Age Bins
age_bins = pd.cut(customers['Age'], bins=[10, 20, 30, 40, 50, 60, 70, 80])
plt.figure(figsize=(10, 6))
sns.countplot(age_bins, palette=color_palette)
plt.xticks(rotation=45)
plt.title('Customer Count in Different Age Groups')
plt.xlabel('Age Group')
plt.ylabel('Count')
plt.savefig('Age_Group_Count.png')
plt.close()

# 6. Violin Plot of Spending Score Across Age Groups
plt.figure(figsize=(10, 6))
sns.violinplot(x=age_bins, y='Spending Score (1-100)', data=customers, palette=color_palette)
plt.title('Distribution of Spending Scores Across Age Groups')
plt.xlabel('Age Group')
plt.ylabel('Spending Score')
plt.savefig('Spending_Score_Across_Age_Groups.png')
plt.close()

# Preprocessing
features = ['Age', 'Annual Income (k$)', 'Spending Score (1-100)']
X = customers[features]
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# PCA for Dimensionality Reduction
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)

# Optimal Number of Clusters using Elbow Method
# K-Means Clustering
kmeans = KMeans(n_clusters=5, init='k-means++', random_state=1)
kmeans.fit(X_pca)
customers['KMeans_Cluster'] = kmeans.labels_

model = KMeans(random_state=1)
visualizer = KElbowVisualizer(model, k=(2,10), metric='silhouette')
visualizer.fit(X_pca)
visualizer.show(outpath="Elbow_Visualization.png")
plt.close()

# K-Means Clustering Visualization
plt.figure(figsize=(10,6))
sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=customers['KMeans_Cluster'], palette='viridis')
plt.title('K-Means Clustering (PCA-reduced data)')
plt.xlabel('PCA Feature 1')
plt.ylabel('PCA Feature 2')
plt.legend(title='Cluster')
plt.savefig('KMeans_Clustering.png')
plt.close()

# DBSCAN Clustering Visualization
dbscan = DBSCAN(eps=0.5, min_samples=5)
customers['DBSCAN_Cluster'] = dbscan.fit_predict(X_pca)
plt.figure(figsize=(10,6))
sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=customers['DBSCAN_Cluster'], palette='viridis')
plt.title('DBSCAN Clustering (PCA-reduced data)')
plt.xlabel('PCA Feature 1')
plt.ylabel('PCA Feature 2')
plt.legend(title='Cluster')
plt.savefig('DBSCAN_Clustering.png')
plt.close()

# Agglomerative Clustering Visualization
agg_clustering = AgglomerativeClustering(n_clusters=5)
customers['Agg_Cluster'] = agg_clustering.fit_predict(X_pca)
plt.figure(figsize=(10,6))
sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=customers['Agg_Cluster'], palette='viridis')
plt.title('Agglomerative Clustering (PCA-reduced data)')
plt.xlabel('PCA Feature 1')
plt.ylabel('PCA Feature 2')
plt.legend(title='Cluster')
plt.savefig('Agglomerative_Clustering.png')
plt.close()

# Spectral Clustering Visualization
spectral = SpectralClustering(n_clusters=5, affinity='nearest_neighbors')
customers['Spectral_Cluster'] = spectral.fit_predict(X_pca)
plt.figure(figsize=(10,6))
sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=customers['Spectral_Cluster'], palette='viridis')
plt.title('Spectral Clustering (PCA-reduced data)')
plt.xlabel('PCA Feature 1')
plt.ylabel('PCA Feature 2')
plt.legend(title='Cluster')
plt.savefig('Spectral_Clustering.png')
plt.close()

# Gaussian Mixture Model Clustering Visualization
gmm = GaussianMixture(n_components=5)
customers['GMM_Cluster'] = gmm.fit_predict(X_pca)
plt.figure(figsize=(10,6))
sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=customers['GMM_Cluster'], palette='viridis')
plt.title('Gaussian Mixture Model Clustering (PCA-reduced data)')
plt.xlabel('PCA Feature 1')
plt.ylabel('PCA Feature 2')
plt.legend(title='Cluster')
plt.savefig('GMM_Clustering.png')
plt.close()