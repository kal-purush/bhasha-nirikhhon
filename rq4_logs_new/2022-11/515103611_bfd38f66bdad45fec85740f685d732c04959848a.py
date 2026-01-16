import os

import numpy as np

p = os.path.sep.join(['saved_models', 'vgg11_cifar10' + '_sgd_ndn', 'vgg11' + '_acc.txt'])
f = open(p)
acc = list(map(float, f.readline().lstrip('[').rstrip(']\n').split(', ')))
print(max(acc))
idx = acc.index(max(acc))
p = os.path.sep.join(['saved_models', 'vgg11_cifar10' + '_sgd_ndn', 'vgg11train' + '_acc.txt'])
f = open(p)
acc1 = list(map(float, f.readline().lstrip('[').rstrip(']\n').split(', ')))
print(acc1[idx])