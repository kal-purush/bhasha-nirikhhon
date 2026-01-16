import matplotlib.pyplot as plt
import numpy as np
from src import kmeans

colors_1d = ['r', 'g', 'b', 'y']
colors_2d = ['r', 'g', 'y']

def step_by_step_visual(points, k, scatter_points, scatter_centroids, scatter_clusters, seed):
    fig, axes = plt.subplots(nrows=4,ncols=2,figsize=(15,5))

    for row in axes:
        for ax in row:
            ax.axis('off')

    scatter_points(axes[0, 0], points)
    axes[0, 0].title.set_text("1. Initial dataset")

    centroids = kmeans.pick_initial_centroids(k, points, seed)
    scatter_points(axes[1, 0], points)
    scatter_centroids(axes[1, 0], centroids)
    axes[1, 0].title.set_text("2. Pick random centroids")

    closest = kmeans.get_closest_centroids(points, centroids)
    clusters = kmeans.cluster_points(points, centroids, closest)
    scatter_clusters(axes[2, 0], clusters)
    scatter_centroids(axes[2, 0], centroids)
    axes[2, 0].title.set_text("3. Assign each element to nearest centroid")

    centroids = kmeans.update_centroids(clusters)
    scatter_clusters(axes[3, 0], clusters)
    scatter_centroids(axes[3, 0], centroids)
    axes[3, 0].title.set_text("4. Recalculate centroids as mean")

    clusters, centroids = kmeans.k_means_cluster_step(points, centroids)
    scatter_clusters(axes[0, 1], clusters)
    scatter_centroids(axes[0, 1], centroids)
    axes[0, 1].title.set_text("5. Repeat #3 and #4 (second iteration)")

    clusters, centroids = kmeans.k_means_cluster_step(points, centroids)
    scatter_clusters(axes[1, 1], clusters)
    scatter_centroids(axes[1, 1], centroids)
    axes[1, 1].title.set_text("6. Iteration 3")

    clusters, centroids = kmeans.k_means_cluster_step(points, centroids)
    scatter_clusters(axes[2, 1], clusters)
    scatter_centroids(axes[2, 1], centroids)
    axes[2, 1].title.set_text("7. Iteration 4")

    clusters, centroids = kmeans.k_means_cluster_step(points, centroids)
    scatter_clusters(axes[3, 1], clusters)
    scatter_centroids(axes[3, 1], centroids)
    axes[3, 1].title.set_text("8. Iteration 5: converged")

def scatter_points_1d(ax, data):
    ax.scatter(data, np.zeros_like(data), s=5)

def scatter_clusters_1d(ax, clusters):
    for i, cluster in enumerate(clusters):
        ax.scatter(cluster, np.zeros_like(cluster), c=colors_1d[i], s=5)
        
def scatter_centroids_1d(ax, centroids):
    ax.scatter(centroids, np.zeros_like(centroids), c=colors_1d, s=100)

def scatter_points_2d(ax, points):
    ax.scatter(points[:,0], points[:,1], s=5)
    
def scatter_centroids_2d(ax, centroids):
    ax.scatter(centroids[:,0], centroids[:,1], s=100, c=colors_2d)
    
def scatter_clusters_2d(ax, clusters):
    for i, cluster in enumerate(clusters):
        ax.scatter(cluster[:,0], cluster[:,1], s=5, c=colors_2d[i])

def step_by_step_1d(points, seed):
    k = 4
    step_by_step_visual(points, k, scatter_points_1d, scatter_centroids_1d, scatter_clusters_1d, seed)

def iterations_2d(points, seed):
    _, axes = plt.subplots(nrows=3,ncols=2,figsize=(15,15))

    k = 3

    for row in axes:
        for ax in row:
            ax.axis('off')
        
    scatter_points_2d(axes[0, 0], points)
    axes[0, 0].title.set_text("1. Initial data")

    centroids = kmeans.pick_initial_centroids(k, points, seed)
    scatter_points_2d(axes[1, 0], points)
    scatter_centroids_2d(axes[1, 0], centroids)
    axes[1, 0].title.set_text("2. Initial centroids")

    clusters, centroids = kmeans.k_means_cluster_step(points, centroids)
    scatter_clusters_2d(axes[2, 0], clusters)
    scatter_centroids_2d(axes[2, 0], centroids)
    axes[2, 0].title.set_text("3. First iteration")

    for i in range(3):
        clusters, centroids = kmeans.k_means_cluster_step(points, centroids)
        scatter_clusters_2d(axes[i, 1], clusters)
        scatter_centroids_2d(axes[i, 1], centroids)
        step = 4 + i
        iteration = 2 + i
        axes[i, 1].title.set_text(str(step) + ". Iteration " + str(iteration))

def k_values(points, nrows, ncols, figsize, scatter_fn, seed):
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize)

    k = 1

    for i in range(ncols):
        for j in range(nrows):
            ax = axes[j, i]
            ax.axis('off')
            clusters, iterations = kmeans.k_means_clustering(points, k, seed)
            for cluster in clusters:
                scatter_fn(ax, cluster)
            ax.title.set_text("K = {}, converged at {} iterations".format(k, iterations))
            k += 1

def compare(clusters, ideal, scatter_fn):
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15,3))

    for cluster in clusters:
        axes[0].axis('off')
        scatter_fn(axes[0], cluster)
        axes[0].title.set_text("K means clustering")

    for cluster in ideal:
        axes[1].axis('off')
        scatter_fn(axes[1], cluster)
        axes[1].title.set_text("Ideal clusters")