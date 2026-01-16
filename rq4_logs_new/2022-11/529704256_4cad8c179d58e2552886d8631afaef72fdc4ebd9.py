import functions.load as fnl
import functions.plot as fnp
import functions.general as fng
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stat
import pandas as pd

# name spaces for namlab nwb extension
namespaces = ["ndx-photometry-namlab.namespace.yaml","ndx-eventlog-namlab.namespace.yaml"]

# DANDI set id
dandiset_id = '000351'

# animal list
mousenumber = ['M2','M3','M4','M5','M6','M7','F1','F2']
mouselist = ['HJ-FP-'+i for i in mousenumber]

# event indices
cs1index = 15
rewardindex = 10
randomrewardindex = 7
lickindex = 5

##
for im, mousename in enumerate(mouselist):
    print(mousename)

    # load DANDI url of an animal
    url, path = fnl.load_dandi_url(dandiset_id, mousename)

    for i,v in enumerate(url):
        # load eventlog and dff from nwb file
        results, _ = fnl.load_nwb(v, namespaces, [('a', 'eventlog'), ('p', 'photometry', 'dff')])

