#!usr/bin/py

import numpy as np
from PIL import Image
from pymod.meme import MemeFFNN
import pymod.prep as prep
import pymod.img as img
import pickle

num_principals = 10
trust = 0
N = 100

rgb_shape = (100,100,3)

elevmap = img.array_BNW('elevmap200.jpg')
landmap = img.array_BNW('landmap200.jpg')
roadmap = img.array_BNW('roadmap200.jpg')
target = img.array_RGB('target200.jpg')

elevmap0 = elevmap[0:100, 0:100]
elevmap1 = elevmap[100:200, 0:100]
elevmap2 = elevmap[0:100, 100:200]
elevmap3 = elevmap[100:200, 100:200]

landmap0 = landmap[0:100, 0:100]
landmap1 = landmap[100:200, 0:100]
landmap2 = landmap[0:100, 100:200]
landmap3 = landmap[100:200, 100:200]

roadmap0 = roadmap[0:100, 0:100]
roadmap1 = roadmap[100:200, 0:100]
roadmap2 = roadmap[0:100, 100:200]
roadmap3 = roadmap[100:200, 100:200]

target0 = target[0:100, 0:100, :].flatten()
target1 = target[100:200, 0:100, :].flatten()
target2 = target[0:100, 100:200, :].flatten()
target3 = target[100:200, 100:200, :].flatten()

elevmap0 = prep.pca(elevmap0, num_principals).T
elev0 = prep.pca(elevmap0, num_principals).T.flatten()
elevmap1 = prep.pca(elevmap1, num_principals).T
elev1 = prep.pca(elevmap1, num_principals).T.flatten()
elevmap2 = prep.pca(elevmap2, num_principals).T
elev2 = prep.pca(elevmap2, num_principals).T.flatten()
elevmap3 = prep.pca(elevmap3, num_principals).T
elev3 = prep.pca(elevmap3, num_principals).T.flatten()

landmap0 = prep.pca(landmap0, num_principals).T
land0 = prep.pca(elevmap0, num_principals).T.flatten()
landmap1 = prep.pca(landmap1, num_principals).T
land1 = prep.pca(elevmap1, num_principals).T.flatten()
landmap2 = prep.pca(landmap2, num_principals).T
land2 = prep.pca(elevmap2, num_principals).T.flatten()
landmap3 = prep.pca(landmap3, num_principals).T
land3 = prep.pca(elevmap3, num_principals).T.flatten()

roadmap0 = prep.pca(roadmap0, num_principals).T
road0 = prep.pca(roadmap0, num_principals).T.flatten()
roadmap1 = prep.pca(roadmap1, num_principals).T
road1 = prep.pca(roadmap1, num_principals).T.flatten()
roadmap2 = prep.pca(roadmap2, num_principals).T
road2 = prep.pca(roadmap2, num_principals).T.flatten()
roadmap3 = prep.pca(roadmap3, num_principals).T
road3 = prep.pca(roadmap3, num_principals).T.flatten()

dataset = np.vstack((
    np.hstack((elev0, land0, road0)),
    np.hstack((elev1, land1, road1)),
    np.hstack((elev2, land2, road2)),
    np.hstack((elev3, land3, road3)),
))

targetset = np.vstack((target0, target1, target2, target3))

# print dataset.shape
net = MemeFFNN(len(dataset[0]), len(target0), 300, 200)
print "initial set mean square error: %f" % (net.set_meansq(dataset, targetset))
net.train_set_N(dataset, targetset, trust, N, True, 250)

fhandle = file('net.pkl', 'wb')
pickle.dump(net, fhandle)
fhandle.close()

output = np.zeros((200,200,3))

output[0:100, 0:100, :] = net.feedforward(dataset[0]).reshape(rgb_shape)
output[100:200, 0:100, :] = net.feedforward(dataset[1]).reshape(rgb_shape)
output[0:100, 100:200, :] = net.feedforward(dataset[2]).reshape(rgb_shape)
output[100:200, 100:200, :] = net.feedforward(dataset[3]).reshape(rgb_shape)

output = np.rint(output)

img.imgArray(output, 'maptest_out.png')