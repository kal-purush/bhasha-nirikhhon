__author__ = 'yutian'


import json
import csv
import numpy as np
from itertools import chain


#### read in all user data ####
data = []
n =0
with open('yelp_academic_dataset_user.json') as f:
    for i in f:
        i = unicode(i)
        data.append(json.loads(i))
        #n = n+1
        #if n > 6000:
        #    break
print "Finished reading!",n

#### keywords that we're interested in ####
kw = [u'user_id', u'name', u'yelping_since', u'votes',  u'compliments', u'fans', u'average_stars', u'review_count', u'friends']

#### get a reformatted data set ####
for i, dt in enumerate(data):

    data[i] = {k:v for k, v in data[i].iteritems() if k in kw}

    data[i][u'yelping_since'] = int(data[i][u'yelping_since'].split('-')[0])

    for k in [u'votes', u'compliments']:
        data[i][k] = sum(data[i][k].values())

    for k in [ u'name']:
        #print data[i][k]
        data[i][k] = data[i][k].encode('utf8')

print "Got raw data!"

#### write reformatted data into a csv file ####
'''
f = open('nlp/yelp_network_all.csv','w')
for dt in data:
    w = csv.DictWriter(f, dt.keys())
    w.writerow(dt)
f.close()
'''

#### subset data to get users with at least one friend ####
data_sub = [{k:v for k, v in data[i].iteritems()} for i in range(len(data)) if len(data[i][u'friends']) > 0]
print "Subsetted!"

adj_info = [{k:v for k, v in data_sub[i].iteritems() if k in [u'user_id',u'friends']} for i in range(len(data_sub))]
adj_info = [{(d.values()[1]): (d.values()[0])} for d in adj_info] # save in dict as [{usr:[friends,....]}, {}, {}, .....]
print "Adjacency list done!"

all_usrs = [d.keys() for d in adj_info]  # from the whole population, get every ego in the network
all_usrs_id = []
for i in all_usrs:
    all_usrs_id.extend(i)
#usr_refr = dict(zip(all_usrs_id, range(len(all_usrs_id))))  # assign each usr_id a reference number for later use
usr_refr = dict(zip(all_usrs_id, range(len(all_usrs_id))))  # assign each usr_id a reference number for later use

print "id_num reference done!", len(all_usrs_id)


## sample population
sample = np.random.random_integers(0, len(all_usrs_id)-1, size=0.8*len(all_usrs_id))

# get adjacency matrix _ sample from huge number of egos
adj_mtx = np.zeros(shape=(len(all_usrs),len(all_usrs)), dtype=int)
#n = 0
'''
for i in sample:
    #print i
    print usr_refr[i]
    i_id = (usr_refr[i]).encode('utf-8')

    J_id = adj_info[0][i_id]
    print J_id
'''

for l in adj_info:
    l_num = usr_refr[l.keys()[0]]
    if l_num in sample:
        i = l_num
        for frd in l.values()[0]:
            if frd in usr_refr.keys():
                j = usr_refr[frd]
                if j in sample:
                    adj_mtx[i,j] = 1
print adj_mtx.shape
dlt = np.sum(adj_mtx, axis=0) < 4
print dlt

adj_mtx = adj_mtx[~dlt]
adj_mtx = adj_mtx[0:, ~dlt]
print "Adjacency matrix done!", adj_mtx.shape, len(dlt),len(data_sub)


### get attribute and adjacent list _ sample from huge number of egos
attribute = [{k:v for k, v in data_sub[i].iteritems() if k != u'friends'} for i in range(len(data_sub))]
attribute = [attribute[i] for i,v in enumerate(dlt) if v !=True]
print "Attributes done!", len(attribute),len(dlt)


### write attribute and adjacent matrix to files
# attribute
f = open('yelp_network_attribute.csv','w')
for dt in attribute:
    w = csv.DictWriter(f, dt.keys())
    w.writerow(dt)
f.close()
print "Finished wiring attributes!"

# adjacent matrix
np.savetxt("yelp_network_adjmtx.csv", adj_mtx, delimiter=",")
print "Finished wiring adjacency matrix!"

'''
f = open('yelp_network_adjmtx.csv','w')
for dt in attribute:
    w = csv.DictWriter(f, dt.keys())
    w.writerow(dt)
f.close()

'''
