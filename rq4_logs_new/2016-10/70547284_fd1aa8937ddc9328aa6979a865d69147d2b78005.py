'''
Accuracies of different prototype selection and metrics
random
    euclidean
        k=5000 0.95
kmeans
    euclidean:
        k=1 0.82
        k=100 
        k=1000 0.957
        k=5000 0.9669
        k=10000 0.9693
    twosided tangent:
        k=10 0.8439
        k=100 0.9667
    onesided tangent: 
        k=100 0.9426
'''

import numpy as np
from PIL import Image
from numpy import linalg as LA
from sklearn.datasets import fetch_mldata
from kmeans import kmeans

mnist = fetch_mldata('MNIST original', data_home='./data')

def imageshow(img, name='', showflag=True):
    im = Image.fromarray(np.reshape(img,(28,28)).astype(np.uint8))
    if showflag:
        im.show()
    if name!='':
        im.save(name)
    return im

def score(test_dataset, test_labels, prototypes, prototype_labels, metric):
    res = np.empty((len(test_dataset)), dtype=np.uint8)
    D = metric(test_dataset, prototypes)
    res = np.argmin(D, axis=1)
    s=np.mean(res==test_labels)
    return res,s 

# TD
from mpdist import mpdist
from kmeans import kmeans

k=10
prototypes=[]
prototype_labels=[]
m1TD = lambda X,Y: mpdist(X, Y, oneSideTD, ("tangentDistance","ctypes","numpy"))
for i in xrange(10):
    print 'clustering %d-th label' %(i)
    train_dataset = mnist.data[mnist.target==i]
    test_dataset = mnist.target[mnist.target==i]
    init_center = np.array(train_dataset[:(k/10)])
    c,cid,dist=kmeans(train_dataset, init_center, metric=m1TD, pdist=True)
    for cc in c: prototypes.append(cc)
    prototype_labels += [i]*(k/10)

cls, s=score(mnist.data[-10000:], mnist.target[-10000:], prototypes, prototype_labels, m1TD)
print s