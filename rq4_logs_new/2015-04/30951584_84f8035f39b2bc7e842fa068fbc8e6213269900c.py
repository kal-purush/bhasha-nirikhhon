__author__ = 'yutian'


import json
import csv
import numpy as np
import matplotlib as pyplot
from itertools import chain
import scipy
from scipy import cluster
from scipy.cluster.vq import vq, kmeans, kmeans2,whiten


data = []
n = 0
with open('yelp_academic_dataset_business.json') as f:
    for i in f:
        i = unicode(i)
        data.append(json.loads(i))
        #n = n+1
        #if n > 10:
        #    break
print "Finished reading!",n

# not differentiating location for now, we may want to cluster within certain geography range
kw = [u'review_count', u'stars', u'attributes', u'categories']

features = [{k:v for k, v in data[i].iteritems() if k in kw} for i, dt in enumerate(data)]
reference = [{k:v for k, v in data[i].iteritems() if k in [u'name', u'business_id']} for i, dt in enumerate(data)]


n=0
for dt in features: # assing each business an id from 1 to len(data)
    dt['id'] = n
    n = n +1

n=0
for dt in reference: # assing each business an id from 1 to len(data)
    dt['id'] = n
    n = n +1

print "Got raw data!"
#print features, "\n",reference


# get the full set of attributes
attributes_all = [dt[u'attributes'].keys() for dt in data]
attributes_all = list(chain(*attributes_all))
attributes_all = list(set(attributes_all))

# get the full set of categories
categories_all = [dt[u'categories'] for dt in data]
categories_all = list(chain(*categories_all))
categories_all = list(set(categories_all))

dimensions = [u'review_count', u'stars']
dimensions.extend(categories_all)
dimensions.extend(attributes_all)
print "Our business objects have features of dimension = ", len(dimensions)

features_list = []  # a list stores features of businesses in a list
for f in features:
    lst = []
    for d in dimensions:
        if d in f.keys():
            lst.extend([f[d]])
        elif d in f[u'attributes'].keys():
            if f[u'attributes'][d]==True:
                lst.extend([1])
            else:
                lst.extend([0])
        elif d in f[u'categories']:
            lst.extend([1])
        else:
            lst.extend([0])
    features_list.append(lst)

features_mtx = np.array(features_list) # convert into ndarray
features_mtx.astype(int)
print "The observation has a shape of", features_mtx.shape
#print (features_mtx)

np.savetxt('business_features_mtx.csv', features_mtx, delimiter=',')
