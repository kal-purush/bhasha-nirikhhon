
import numpy as np
import pandas as pd
import openpyxl
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import math
from math import ceil

# Linear Regression:
import statsmodels.api as sm

# Decision Tree:
from sklearn.tree import DecisionTreeClassifier, export_graphviz
import graphviz
import pydotplus

# Pre-processing:
import sklearn.preprocessing as prep

# Clustering:
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.base import clone
from yellowbrick.cluster import KElbowVisualizer
from sklearn.metrics import silhouette_samples, silhouette_score
from scipy.cluster.hierarchy import dendrogram, linkage

# t-SNE:
from sklearn.manifold import TSNE

# Statistical hypothesis testing:
from scipy.stats import shapiro, f_oneway, ttest_ind

import warnings
warnings.filterwarnings('ignore')

data = pd.read_excel('/Users/rushabh/DataScienceProject/data_covid_usa.xlsx', 'data', engine='openpyxl')

data.head()
# Get a copy of the original dataset
data_original = data.copy()
# Dimension of the dataset (it has 51 rows because it includes the 50 States + the deferal District Of Columbia)
data.shape
# Data types of the variables and columns - All data types are already correct!
data.dtypes
# Summary Statistics of our variables
data.describe(include='all').T.drop(columns=['unique', 'top', 'freq'])

# Info on the USA COVID-19 dataset
data.info()
# Check duplicates
any(data.duplicated())

# Count of missing values
data.isna().sum()
# state: stays as index of the dataset
data.set_index('state', inplace=True)

# Drop the state codes from the main dataset
data.drop(columns='state_code', inplace=True)

# Update the variable pc_republican_votes to have the percentages in the range 0-100, like the other percentage columns
data['pc_republican_votes'] = data['pc_republican_votes'] * 100


data.head()
sns.set(style='whitegrid')

# Create individual axes
fig, axes = plt.subplots(ceil(len(data.columns) / 2), 2, figsize=(15, 45))
plt.subplots_adjust(hspace=0.18)

# Plot data
for ax, feat in zip(axes.flatten(), data.columns):
    ax.hist(data[feat], color='lightgreen')
    ax.set_title(feat)

# Show
plt.show()

sns.set(style='whitegrid')

# Prepare dataframe layout
plot_data = data.reset_index().melt('state')
plot_features = data.reset_index().drop('state', 1).columns

# Prepare figure layout
fig, axes = plt.subplots(1, len(plot_features[:16]), figsize=(15, 8), constrained_layout=True)

# Draw the boxplots
for i in zip(axes, plot_features[:16]):
    sns.boxplot(x='variable', y='value', data=plot_data.loc[plot_data['variable'] == i[1]], ax=i[0], color='lightgreen')
    i[0].set_xlabel('')
    i[0].set_ylabel('')

# Finalize the plot
plt.suptitle("Variables' box plots", fontsize=25)
sns.despine(bottom=True)

plt.show()


# %% md

# %%

def color_red_or_green(val):
    if val < -0.8:
        color = 'background-color: red'
    elif val > 0.8:
        color = 'background-color: green'
    else:
        color = ''
    return color


# Checking correlations
correlations = data.corr()
correlations.style.applymap(color_red_or_green)

data = data[data['pop_density']<3950]

sns.set(style='whitegrid')

# Prepare dataframe layout
plot_data = data.reset_index().melt('state')
plot_features = data.reset_index().drop('state', 1).columns

# Prepare figure layout
fig, axes = plt.subplots(1, len(plot_features[:16]), figsize=(15, 8), constrained_layout=True)

# Draw the boxplots
for i in zip(axes, plot_features[:16]):
    sns.boxplot(x='variable', y='value', data=plot_data.loc[plot_data['variable'] == i[1]], ax=i[0], color='lightgreen')
    i[0].set_xlabel('')
    i[0].set_ylabel('')

# Finalize the plot
plt.suptitle("Variables' box plots", fontsize=25)
sns.despine(bottom=True)

plt.show()

data['icu_bed_1T_pop'] = (data['icu_bed']*1000) / data['pop_size']
data['hospital_bed_1T_pop'] = (data['hospital_bed']*1000) / data['pop_size']

data['health_professionals'] = data['nurses'] + data['medical_doctors']
data['health_professionals_1T_pop'] = (data['health_professionals']*1000) / data['pop_size']

data.drop(columns=['pop_size', 'hospital_bed', 'icu_bed', 'nurses', 'medical_doctors', 'health_professionals'],\
          inplace=True)
sns.set(style='whitegrid')

# Prepare dataframe layout
plot_data = data.reset_index().melt('state')
plot_features = data.reset_index().drop('state', 1).columns

#Prepare figure layout
fig, axes = plt.subplots(1, len(plot_features[:16]), figsize=(15,8), constrained_layout=True)

# Draw the boxplots
for i in zip(axes, plot_features[:16]):
    sns.boxplot(x='variable', y='value', data=plot_data.loc[plot_data['variable']==i[1]], ax=i[0], color='lightgreen')
    i[0].set_xlabel('')
    i[0].set_ylabel('')

# Finalize the plot
plt.suptitle("Variables' box plots", fontsize=25)
sns.despine(bottom=True)

plt.show()
sns.set(style='white')

# Compute the correlation matrix
corr = data.corr()

# Generate a mask for the upper triangle
mask = np.zeros_like(corr, dtype=np.bool) # Return an array of zeros (Falses) with the same shape and type as a given array
mask[np.triu_indices_from(mask)] = True # The upper-triangle array is now composed by True values

# Set up the matplotlib figure
fig, ax = plt.subplots(figsize=(12, 8))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(220, 10, as_cmap=True) # Return a matplotlib colormap object.

# Draw the heatmap with the mask and correct aspect ratio
sns.heatmap(corr, cmap=cmap, center=0, square=True, mask=mask, linewidths=.5, ax=ax, fmt="s")

# Layout
plt.subplots_adjust(top=0.95)
plt.suptitle('Correlation matrix', fontsize=25)
plt.yticks(rotation=0)

# Fixing a bug
b, t = plt.ylim() # Discover the values for bottom and top
b += 0.5 # Add 0.5 to the bottom
t -= 0.5 # Subtract 0.5 from the top
plt.ylim(b, t) # Update the ylim(bottom, top) values
plt.show()
sns.set(style='white')

# Compute the correlation matrix
corr = data.corr()

# Generate a mask for the upper triangle
mask = np.zeros_like(corr, dtype=np.bool) # Return an array of zeros (Falses) with the same shape and type as a given array
mask[np.triu_indices_from(mask)] = True # The upper-triangle array is now composed by True values

# Set up the matplotlib figure
fig, ax = plt.subplots(figsize=(12, 8))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(220, 10, as_cmap=True) # Return a matplotlib colormap object.

# Pass 2D Numpy array to annot parameter
mask_annot = np.absolute(corr.values)>=0.70 # Annotate correlations above abs(0.7) or below abs(0.05)
annot_arr = np.where(mask_annot, corr.values.round(2), np.full((12,12),""))

# Draw the heatmap with the mask and correct aspect ratio
sns.heatmap(corr, cmap=cmap, center=0, square=True, mask=mask, linewidths=.5, ax=ax, annot=annot_arr, fmt="s")

# Layout
plt.subplots_adjust(top=0.95)
plt.suptitle('Correlation matrix', fontsize=25)
plt.yticks(rotation=0)

# Fixing a bug
b, t = plt.ylim() # Discover the values for bottom and top
b += 0.5 # Add 0.5 to the bottom
t -= 0.5 # Subtract 0.5 from the top
plt.ylim(b, t) # Update the ylim(bottom, top) values
plt.show()print('The correlation between hospital_bed_1T_pop and the dependent variable is:',
      round(corr.loc['hospital_bed_1T_pop', 'deaths_1M_pop'], 2))
print('The correlation between icu_bed_1T_pop and the dependent variable is:',
      round(corr.loc['icu_bed_1T_pop', 'deaths_1M_pop'], 2))
data.drop(columns='hospital_bed_1T_pop', inplace=True)

scaler = prep.StandardScaler()
data_std = pd.DataFrame(scaler.fit_transform(data), index=data.index, columns=data.columns)

dep_var = data_std['deaths_1M_pop']
indep_vars = data_std.drop(columns = 'deaths_1M_pop')

dep_var_orig = data['deaths_1M_pop']
indep_vars_orig = data.drop(columns = 'deaths_1M_pop')ols_reg = sm.OLS(dep_var_orig, sm.add_constant(indep_vars_orig))

result = ols_reg.fit()

print(result.summary())
ols_reg_std = sm.OLS(dep_var, sm.add_constant(indep_vars)) # Normalized data!

result_std = ols_reg_std.fit()

print(result_std.summary())
print('R²:', round(result_std.rsquared, 3))
print('Adjusted R²:', round(result_std.rsquared_adj, 3))
dep_var_class = dep_var_orig.reset_index()

# '0' if the state has less than the median in terms of deaths per 1 million people or the same, '1' otherwise
dep_var_class['class'] = 0
dep_var_class.loc[dep_var_class['deaths_1M_pop'] > dep_var_class['deaths_1M_pop'].median(), 'class'] = 1

# Final Pandas Series to be used as the dependent variable on the Decision Tree
dep_var_class = dep_var_class.set_index('state')['class']
def plot_tree(model):
    dot_data = export_graphviz(model,
                               feature_names=indep_vars_orig.columns,
                               class_names=["Below Median", "Above Median"],
                               filled=True)
    pydot_graph = pydotplus.graph_from_dot_data(dot_data)
    pydot_graph.set_size('"10,10"')
    return graphviz.Source(pydot_graph.to_string())
decision_tree = DecisionTreeClassifier(max_depth=5).fit(indep_vars_orig, dep_var_class)
print('Tree\'s Accuracy:', decision_tree.score(indep_vars_orig, dep_var_class)*100, '%')
plot_tree(decision_tree)

def get_r2_scores(df, clusterer, min_k=2, max_k=11):
    """
    Loop over different values of k. To be used with sklearn clusterers.
    """
    r2_clust = {}
    for n in range(min_k, max_k):
        clust = clone(clusterer).set_params(n_clusters=n)
        labels = clust.fit_predict(df)
        r2_clust[n] = r2(df, labels)
    return r2_clust

def r2(df, labels):
    sst = get_ss(df)
    ssw = np.sum(df.groupby(labels).apply(get_ss))
    return 1 - ssw/sst

def get_ss(df):
    """
    Computes the sum of squares for all variables given a dataset.
    """
    ss = np.sum(df.var() * (df.count() - 1))
    return ss # Return sum of sum of squares of each df variable


def r_sq_plot(df):
    r2_scores = {}

    hierarchical = AgglomerativeClustering(affinity='euclidean')

    # Doing hierarchical on top of the K-means
    for linkage in ['complete', 'average', 'single', 'ward']:
        r2_scores[linkage] = get_r2_scores(
            df, hierarchical.set_params(linkage=linkage)
        )

    # Visualizing the R² scores for each cluster solution
    pd.DataFrame(r2_scores).plot.line(figsize=(10, 7))

    plt.title("R² plot for various clustering methods\n", fontsize=21)
    plt.legend(title="Cluster methods", title_fontsize=11)
    plt.xlabel("Number of clusters", fontsize=13)
    plt.ylabel("R² metric", fontsize=13)
    plt.show()


def kelbowplots(clus, data):
    """
    This function produces the k-elbow plot for a given clustering estimator according to 2 quality measures:
    Silhouette and Calinski Harabasz.
    """
    sns.set()
    fig, axes = plt.subplots(1, 2, figsize=(15,6))

    # Plot the data
    metrics = ["silhouette", "calinski_harabasz"]
    ylabels = ["Silhouette", "Calinski and Harabasz"]
    titles = ["Silhouette measure plot", "Calinski and Harabasz measure plot"]
    for ax, m, ylabel, title in zip(axes.flatten(), metrics, ylabels, titles):
        # KElbowVisualizer produces the elbow plot for several quality measures
        KElbowVisualizer(clus, metric=m, timings=False, locate_elbow=True, ax=ax).fit(data)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_xlabel("Number of clusters", fontsize=12)
        ax.set_title(title, fontsize=12)

    # Top title
    plt.suptitle("K-Elbow Plots", fontsize=20)

    plt.show()


def silhouette_analysis(df, estimator, shape, figsize, max_nclus, min_nclus=2, dist="euclidean"):
    """
    This function builds the Silhouette plots for a given range of cluster solutions. This is useful to find out the
    most appropriate number of clusters.
    """
    range_n_clusters = list(range(min_nclus, max_nclus + 1))

    sns.set()
    fig, axes = plt.subplots(nrows=shape[0], ncols=shape[1], figsize=figsize)

    if len(range_n_clusters) == 1:  # In case we want to seed the silhouette for just one cluster solution
        axes_final = [axes]
    else:
        axes_final = axes.flatten()

    n = []
    avgs = []
    for ax, n_clusters in zip(axes_final, range_n_clusters):
        # Get parameter that defines number of clusters
        if "n_clusters" in estimator.get_params():
            param = "n_clusters"

        else:
            print("Estimator has no parameter to define number of clusters")
            return None  # To stop if this happens

        # Get the cluster labels by applying the algorithm
        clustering = estimator.set_params(**{param: n_clusters})  # Set the parameters of the estimator
        labels = clustering.fit_predict(df)

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed clusters
        n.append(n_clusters)
        silhouette_avg = silhouette_score(df, labels, metric=dist)
        avgs.append(silhouette_avg)

        # Compute the silhouette scores for each sample
        sample_silhouette_values = silhouette_samples(df, labels, metric=dist)

        y_lower = 10
        for i in range(n_clusters):
            # Aggregate the silhouette scores for samples belonging to cluster i, and sort them
            ith_cluster_silhouette_values = sample_silhouette_values[labels == i]
            ith_cluster_silhouette_values.sort()

            # Get y_upper to demarcate silhouette y range size
            size_cluster_i = ith_cluster_silhouette_values.shape[0]
            y_upper = y_lower + size_cluster_i

            # Filling the silhouette
            color = cm.nipy_spectral(float(i) / n_clusters)
            ax.fill_betweenx(np.arange(y_lower, y_upper),
                             0, ith_cluster_silhouette_values,
                             facecolor=color, edgecolor=color, alpha=0.7)

            # Label the silhouette plots with their cluster numbers at the middle
            ax.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

            # Compute the new y_lower for next plot
            y_lower = y_upper + 10  # 10 for the 0 samples

        ax.set_title("{} Clusters".format(n_clusters), fontsize=13)

        ax.set_xlabel("The silhouette coefficient values")
        ax.set_ylabel("Cluster label")

        ax.set_yticks([])  # Clear the y axis labels / ticks
        ax.axvline(x=silhouette_avg, color="red", linestyle="--", label="Average Silhouette")
        # The vertical line for average silhouette score of all the values

    plt.subplots_adjust(hspace=0.35)
    plt.suptitle(("Clustering Silhouette Plots"), fontsize=23)
    plt.show()

    for i in range(len(n)):
        print(f"For n_clusters = {n[i]}, the average silhouette_score is: {round(avgs[i], 4)}")


def cluster_profiles(df, label_columns, figsize, compar_titles=None):
    """
    Pass df with labels columns of one or multiple clustering labels.
    Then specify this label columns to perform the cluster profile according to them.
    """
    if compar_titles == None:
        compar_titles = [""] * len(label_columns)

    sns.set()
    fig, axes = plt.subplots(nrows=len(label_columns), ncols=2, figsize=figsize, squeeze=False)
    for ax, label, titl in zip(axes, label_columns, compar_titles):
        # Filtering df
        drop_cols = [i for i in label_columns if i != label]
        dfax = df.drop(drop_cols, axis=1)

        # Getting the cluster centroids and counts
        centroids = dfax.groupby(by=label, as_index=False).mean()
        counts = dfax.groupby(by=label, as_index=False).count().iloc[:, [0, 1]]
        counts.columns = [label, "counts"]

        # Setting Data
        pd.plotting.parallel_coordinates(centroids, label, color=sns.color_palette(), ax=ax[0])
        sns.barplot(x=label, y="counts", data=counts, ax=ax[1])

        # Setting Layout
        handles, _ = ax[0].get_legend_handles_labels()
        cluster_labels = ["Cluster {}".format(i) for i in range(len(handles))]
        ax[0].annotate(text=titl, xy=(0.95, 1.1), xycoords='axes fraction', fontsize=13, fontweight='heavy')
        ax[0].legend(handles, cluster_labels)  # Adaptable to number of clusters
        ax[0].axhline(color="black", linestyle="--")
        ax[0].set_title("Cluster Means - {} Clusters".format(len(handles)), fontsize=13)
        ax[0].set_xticklabels(ax[0].get_xticklabels(), rotation=-20)
        ax[1].set_xticklabels(cluster_labels)
        ax[1].set_xlabel("")
        ax[1].set_ylabel("Absolute Frequency")
        ax[1].set_title("Cluster Sizes - {} Clusters".format(len(handles)), fontsize=13)

    plt.subplots_adjust(hspace=0.4, top=0.90)
    plt.suptitle("Cluster Simple Profilling", fontsize=23)
    plt.show()


# %% md

# %%

# Instantiate a K-Means model
clus_estim = KMeans(random_state=42)  # Default is already 'k-means++' on init, and 10 on n_init, which is good

# K-elbow plots
kelbowplots(clus_estim, indep_vars)


kmeans = KMeans(random_state=42)
silhouette_analysis(indep_vars, kmeans, (2,2), (20,12), max_nclus=6)

# Perform k-means
model_km = KMeans(3, random_state=42)
model_km.fit(indep_vars)
clust_labels_km = model_km.predict(indep_vars)
cent_km = model_km.cluster_centers_


# Appending the cluster labels to a new dataframe
indep_vars_kmeans_final = indep_vars.copy()
indep_vars_kmeans_final['k_means_labels'] = clust_labels_km


# Implementing t-SNE
two_dim = TSNE(random_state=42).fit_transform(indep_vars_kmeans_final.drop(columns='k_means_labels'))

two_dim_final = pd.DataFrame(two_dim, index=indep_vars_kmeans_final.index)\
                .merge(indep_vars_kmeans_final['k_means_labels'], on='state')

two_dim_final.columns = ['x', 'y', 'K-means labels']


sns.set(style='whitegrid')

plt.figure(figsize=(10, 8))

# t-SNE visualization
sns.scatterplot(data=two_dim_final, x='x', y='y', hue='K-means labels', s=75, palette='deep')

# Layout
plt.title('K-means Cluster visualization using t-SNE', size=23)
plt.xlim(None, 110)

# Label data points on the scatter plot
def label_point(x, y, val, ax):
    a = pd.concat({'x': x, 'y': y, 'val': val}, axis=1)
    for i, point in a.iterrows():
        ax.text(point['x']+1.5, point['y'], str(point['val']))

two_dim_labels = two_dim_final.reset_index()
label_point(two_dim_labels.x, two_dim_labels.y, two_dim_labels.state, plt.gca())

# # Save it as a png file
# plt.savefig('kmeans_cluster_visualization_using_t_sne.png')

plt.show()

# R-square Plot
r_sq_plot(indep_vars)


# Hierarchical clustering assessment using scipy
linkage_matrix = linkage(indep_vars, method="ward")

# Plot the corresponding Dendrogram
sns.set(style='whitegrid')
fig = plt.figure(figsize=(16,7))

y_threshold = 9
dendrogram(linkage_matrix, labels=indep_vars.index, color_threshold=y_threshold, above_threshold_color='k')

# plt.hlines(y_threshold, 0, 10000, colors="r", linestyles="dashed")
plt.title('Hierarchical Clustering - Ward\'s Dendrogram', fontsize=21)
plt.xlabel('US States', fontsize=13)
plt.ylabel('Euclidean Distance', fontsize=13)
plt.gca().tick_params(axis='x', which='major', labelsize=13.5)

plt.show()


# Hierarchical clustering with linkage according to the plot above
hc = AgglomerativeClustering(linkage='ward')

# K-elbow plots
kelbowplots(hc, indep_vars)


hc_estimator = AgglomerativeClustering(linkage='ward')
silhouette_analysis(indep_vars, hc_estimator, (2,2), (20, 12), 5)

# Perform the final HC
model_hc = AgglomerativeClustering(n_clusters=3, linkage='ward')
clust_labels_hc = model_hc.fit_predict(indep_vars)

# Appending the cluster labels to a new dataframe
indep_vars_hc_final = indep_vars.copy()
indep_vars_hc_final['hc_labels'] = clust_labels_hc

# K-means Cluster Profilling
cluster_profiles(indep_vars_kmeans_final, ['k_means_labels'], (23, 7))


# Exact number of states on each Cluster
indep_vars_kmeans_final['k_means_labels'].value_counts()

#%%

# Average deaths (dependent variable!) on each Cluster
indep_vars_kmeans_final.merge(dep_var, on='state').groupby('k_means_labels')['deaths_1M_pop'].mean()

sns.set(style='whitegrid')

# Define the 3 dataframes
indep_vars_kmeans_final_0 = indep_vars_kmeans_final.merge(dep_var, on='state')[['k_means_labels', 'deaths_1M_pop']][indep_vars_kmeans_final['k_means_labels']==0]
indep_vars_kmeans_final_1 = indep_vars_kmeans_final.merge(dep_var, on='state')[['k_means_labels', 'deaths_1M_pop']][indep_vars_kmeans_final['k_means_labels']==1]
indep_vars_kmeans_final_2 = indep_vars_kmeans_final.merge(dep_var, on='state')[['k_means_labels', 'deaths_1M_pop']][indep_vars_kmeans_final['k_means_labels']==2]

#Prepare figure layout
fig, axes = plt.subplots(1, 3, figsize=(8,8), sharey='row', constrained_layout=True)

# Box plot for Cluster 0
plot_data_0 = indep_vars_kmeans_final_0.reset_index().melt('state')
plot_features_0 = indep_vars_kmeans_final_0.reset_index().drop('state', 1).columns

sns.boxplot(y='value', data=plot_data_0.loc[plot_data_0['variable']=='deaths_1M_pop'], ax=axes[0], color='palegreen')
axes[0].set_xlabel('Cluster 0')
axes[0].set_ylabel('')

# Box plot for Cluster 1
plot_data_1 = indep_vars_kmeans_final_1.reset_index().melt('state')
plot_features_1 = indep_vars_kmeans_final_1.reset_index().drop('state', 1).columns

sns.boxplot(y='value', data=plot_data_1.loc[plot_data_1['variable']=='deaths_1M_pop'], ax=axes[1], color='greenyellow')
axes[1].set_xlabel('Cluster 1')
axes[1].set_ylabel('')

# Box plot for Cluster 2
plot_data_2 = indep_vars_kmeans_final_2.reset_index().melt('state')
plot_features_2 = indep_vars_kmeans_final_2.reset_index().drop('state', 1).columns

sns.boxplot(y='value', data=plot_data_2.loc[plot_data_2['variable']=='deaths_1M_pop'], ax=axes[2], color='forestgreen')
axes[2].set_xlabel('Cluster 2')
axes[2].set_ylabel('')

# Finalize the plot
plt.suptitle('COVID-19 Deaths per 1 million citizens, Distribution on each Cluster', fontsize=23)
sns.despine(bottom=True)

# # Save it as a png file
# plt.savefig('distribution_on_each_cluster.png')

plt.show()