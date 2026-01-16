#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 22:35:44 2020

@author: qwang
"""

import pickle
import pandas as pd
import os
from tqdm import tqdm
data_dir = '/media/mynewdrive/rob/'
os.chdir(data_dir)

#%%
df = pd.read_pickle('data/rob_info_a.pkl')  # From rob_kiwi/data_helper.py
 
# Get regex label
for i, row in tqdm(df.iterrows()): 
    # Read string list from pkl file
    pkl_path = os.path.join('data/rob_str', df.loc[i,'goldID']+'.pkl') 
    with open(pkl_path, 'rb') as fin:
        sent_ls = pickle.load(fin)       
    # Extract text    
    text = ''
    for l in sent_ls:
        t = ' '.join(l)
        text = text + t    


#%% SGD, Random Forest, XGBoost