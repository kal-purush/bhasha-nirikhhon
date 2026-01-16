import time
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from sklearn import svm
from sklearn.datasets import make_blobs
from sklearn.covariance import EllipticEnvelope
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor

matplotlib.rcParams['contour.negative_linestyle'] = 'solid'

if __name__ == '__main__':

  # Example settings
  n_samples = 300
  outliers_fraction = 0.15
  n_outliers = int(outliers_fraction * n_samples)
  n_inliers = n_samples - n_outliers

  # Define outlier/anomaly detection methods to be compared
  anomaly_algorithms = [
    ("Robust covariance", EllipticEnvelope(contamination=outliers_fraction)),
    ("One-Class SVM", svm.OneClassSVM(nu=outliers_fraction, kernel="rbf",
                                      gamma=0.1)),
    ("Isolation Forest", IsolationForest(contamination=outliers_fraction,
                                         random_state=42)),
    ("Local Outlier Factor", LocalOutlierFactor(
        n_neighbors=35, contamination=outliers_fraction))]

  # Define datasets - 2D
  blobs_params = dict(random_state=0, n_samples=n_inliers, n_features=2)
  X = make_blobs(centers=[[0, 0], [0, 0]], cluster_std=0.5, **blobs_params)[0]

  # Add outliers
  rng = np.random.RandomState(42)
  X = np.concatenate([X, rng.uniform(low=-6, high=6,
                        size=(n_outliers, 2))], axis=0)

  # Compare given classifiers under given settings
  xx, yy = np.meshgrid(np.linspace(-7, 7, 150),
                      np.linspace(-7, 7, 150))

  plt.figure(figsize=(len(anomaly_algorithms)*4, 5))
  plt.subplots_adjust(left=.02, right=.98, bottom=.001, top=.96, wspace=.05,
                      hspace=.01)

  # Loop
  plot_num = 1
  for name, algorithm in anomaly_algorithms:
    t0 = time.time()
    algorithm.fit(X)
    t1 = time.time()
    plt.subplot(1, len(anomaly_algorithms), plot_num)
    plt.title(name, size=10)

    # fit the data and tag outliers
    if name == "Local Outlier Factor":
      y_pred = algorithm.fit_predict(X)
    else:
      y_pred = algorithm.fit(X).predict(X)

    # plot the levels lines and the points
    if name != "Local Outlier Factor":  # LOF does not implement predict
      Z = algorithm.predict(np.c_[xx.ravel(), yy.ravel()])
      Z = Z.reshape(xx.shape)
      plt.contour(xx, yy, Z, levels=[0], linewidths=2, colors='black')

    colors = np.array(['#377eb8', '#ff7f00'])
    plt.scatter(X[:, 0], X[:, 1], s=10, color=colors[(y_pred + 1) // 2])

    plt.xlim(-7, 7)
    plt.ylim(-7, 7)
    plt.xticks(())
    plt.yticks(())
    plt.text(.99, .01, ('%.2fs' % (t1 - t0)).lstrip('0'),
            transform=plt.gca().transAxes, size=10,
            horizontalalignment='right')
    plot_num += 1

  # Save
  plt.savefig('./im/anomaly_detection2d.png', bbox_inches = 'tight', pad_inches = 0)

  # Show
  plt.show()