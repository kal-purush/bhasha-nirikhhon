import numpy as np
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
plt.tight_layout()
# the most basic verstion
# n_cluster: number of center clusters
# max_iter: if tolerance condition doesn't occur, the algorithm will stop
# tol: the acceptable difference between previous and current centroids


class KMeans:
    # the defaults based on scikit-learn, which are sensible
    def __init__(self, n_clusters=8, max_iter=300, random_state=42, tol=1e-4):
        self.tol = tol
        self.n_clusters = n_clusters
        self.max_iter = max_iter
        self.random_state = random_state

    def initialize_centroid(self, X):
        np.random.seed(self.random_state)
        random_idx = np.random.choice(X.shape[0], self.n_clusters)
        centroids = X[random_idx]
        return centroids

    def compute_distance(self, X, centroids):
        distance = np.zeros((X.shape[0], self.n_clusters))
        for k in range(self.n_clusters):
            distance[:, k] = np.square(
                np.linalg.norm(X - centroids[k, :], axis=1))

        return distance

    def find_closest_cluster(self, distance):
        return np.argmin(distance, axis=1)

    def compute_centroids(self, X, labels):
        centroids = np.zeros((self.n_clusters, X.shape[1]))
        for k in range(self.n_clusters):
            centroids[k, :] = np.mean(X[labels == k, :], axis=0)

        return centroids

    def compute_sse(self, X, labels, centroids):
        error = np.zeros(X.shape[0])
        for k in range(self.n_clusters):
            error[labels == k] = np.linalg.norm(
                X[labels == k] - centroids[k], axis=1)

        return np.sum(np.square(error))

    def fit(self, X):
        X = np.array(X) if not isinstance(X, np.ndarray) else X

        self.centroids = self.initialize_centroid(X)
        for i in range(self.max_iter):
            old_centroids = self.centroids
            distance = self.compute_distance(X, old_centroids)
            self.labels = self.find_closest_cluster(distance)
            self.centroids = self.compute_centroids(X, self.labels)
            if np.allclose(self.centroids, old_centroids, rtol=0, atol=self.tol):
                break

        self.error = self.compute_sse(X, self.labels, self.centroids)

    def predict(self, X):
        X = np.array(X) if not isinstance(X, np.ndarray) else X
        distance = self.compute_distance(X, self.centroids)
        return self.find_closest_cluster(distance)


# for the sake of visualizing
X = [[1, 2], [2, 3], [3, 1], [6, 5], [7, 7], [8, 6], [6.5, 3], [7, 2], [8, 3]]
X_test = [[6, 8], [4, 4], [7, 2.5]]

kmeans = KMeans(3, 100)
kmeans.fit(X)
results = kmeans.predict(X_test)
print(results)

[plt.scatter(X[i][0], X[i][1], color='b') for i in range(len(X))]
[plt.scatter(kmeans.centroids[i][0], kmeans.centroids[i][1],
             marker="*", s=200) for i in range(len(kmeans.centroids))]
plt.show()