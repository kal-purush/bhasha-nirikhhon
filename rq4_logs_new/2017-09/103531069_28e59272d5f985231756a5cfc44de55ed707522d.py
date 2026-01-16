from sklearn.datasets import fetch_olivetti_faces
from sklearn.decomposition import PCA
from matplotlib import pyplot as plt
import numpy as np

image_shape = (64, 64)

def plot_gallery(title, images, n_col, n_row):
    plt.figure(figsize=(2. * n_col, 2.26 * n_row))
    plt.suptitle(title, size=16)
    for i, comp in enumerate(images):
        plt.subplot(n_row, n_col, i + 1)
        vmax = max(comp.max(), -comp.min())
        plt.imshow(comp.reshape(image_shape), cmap=plt.cm.gray, interpolation='nearest', vmin=-vmax, vmax=vmax)
        plt.xticks(())
        plt.yticks(())
    plt.subplots_adjust(0.01, 0.05, 0.99, 0.93, 0.04, 0.)


dataset = fetch_olivetti_faces()
faces = dataset.data

pca = PCA(5)
pca.fit(faces)
components = pca.components_
means = pca.mean_

np.savetxt('components.txt', components)
np.savetxt('means.txt', means)

pcs = pca.transform(faces[0, :].reshape(1, -1))
print(pcs)

new_face = pca.inverse_transform(pcs)

print(new_face)
# plot_gallery("New face", new_face, 1, 1)
# plt.show()