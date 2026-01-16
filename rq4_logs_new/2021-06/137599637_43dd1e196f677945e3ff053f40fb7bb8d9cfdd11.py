## Ford Fishman
## processEMP.py
## 5/21/21

import pandas as pd
import numpy as np
import multiprocessing as mp
from functools import partial

def map_proc(line, samples, env_dict)->bool:
    
    line = line.strip()
    parts = line.split("\t")
    otu = parts[1:-1]

    vals = np.array([ -1 if env_dict[sample]=="Free-living" else 1 for sample in samples ]) # -1 if freeliving, 1 if host associated

    env_type = vals * np.array(otu)
       
    return (env_type > 0).all()

emp_path = "/geode2/home/u030/ffishman/Carbonate/GitHub/MicroSpeciation/data/EMP/"
sbs_path = emp_path + "emp_sbs.txt"
meta_path = emp_path + "emp_qiime_mapping_release1_20170912.tsv"

meta = pd.read_csv(meta_path, delimiter='\t')
meta = meta.rename(columns = {'#SampleID':'SampleID'})
nrows = meta.shape[0]

env_dict = dict()

for i in range(nrows):

    sample = meta.SampleID[i]
    env = meta.empo_1[i]
    env_dict[sample] = env

with open(sbs_path) as r:
    line1 = r.readline()
    line2 = r.readline().strip()
    otus = r.readlines()

sites = line2.split("\t")[1:-1] # dont include tax information

otu_table = dict()

part_proc = partial(map_proc, samples=sites, env_dict=env_dict)

pool = mp.Pool(mp.cpu_count())

print("%s Processors\n" % mp.cpu_count() )

envs = pool.map(part_proc, otus,chunksize=1)

n = len(envs)

ratio = sum(envs)/n

print(ratio)