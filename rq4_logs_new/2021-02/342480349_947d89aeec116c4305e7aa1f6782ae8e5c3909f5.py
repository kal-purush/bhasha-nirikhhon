#!~/virtualEnvs/anaconda3/envs/imageproc35/bin/python
"""
Author: Garrett Gaskins
Date Initialized: 2017-01-24
Last Edited: 2019-02-21
Email: gaskins@keiserlab.org

Summary: Utility functions for running portions of predict.py and compare_to_ligands.py
""" 

###########################################################################################################################################
#        #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       # 
#                                                                IMPORT MODULES                                                            
#    #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       #     
###########################################################################################################################################

# I/O Handling
import os,sys
import csv
import gzip

# Data Handling
import numpy as np
import pandas as pd

# Plotting
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('darkgrid')


###########################################################################################################################################
#        #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       # 
#                                                              PRIMARY FUNCTIONS                                                           
#    #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       #       #     
###########################################################################################################################################


def map_target_identifiers(chembl_targ_f):
    """Function for mapping target names, so that uniprotID or targ_desc are
    used before generic uninterpretable Chembl_ID."""
    chid_to_targName = {}
    print('Mapping target chembl_IDs to human-readable Names...')
    with gzip.open(chembl_targ_f, 'r') as fi:
        reader = csv.reader(fi)
        reader.next() # Iterate over file header
        for chid, unid, cpd_assocs, tdesc in reader:
            if unid != '':
                chid_to_targName[chid] = unid
            elif tdesc != '':
                chid_to_targName[chid] = tdesc
            else:
                chid_to_targName[chid] = chid
    print('\tMapped {} chembl_IDs'.format(len(chid_to_targName)))
    print('')
    return chid_to_targName


def flatten_list(l):
    """Util function for flattening list of lists into a single list"""
    flat_list = [item for sublist in l for item in sublist]
    return flat_list


def view_target_dist(preds_f, targ_limit=15, figsize=(30,20)):
    """Create bar chart of top predicted targets."""
    # Setup Figure
    fig = plt.figure(figsize=figsize)
    plt.subplot(111)
    fig.subplots_adjust(bottom=0.2)
    # Generate pandas dataframe of 
    predsDF = pd.read_csv(preds_f, usecols=['Probability_Estimate', 'Targ_Class'])
    targ_counts = predsDF['Targ_Class'].value_counts()
    xvals = targ_counts.index.values[0:targ_limit]
    yvals = targ_counts.values[0:targ_limit]
    sns.barplot(x=xvals[::-1], y=yvals[::-1], palette='GnBu')
    plt.title('Top {} Predicted Targets'.format(targ_limit), fontsize=26)
    plt.xlabel('Target Prediction', fontsize=17)
    plt.ylabel('Frequency of Prediction', fontsize=20)
    plt.xticks(rotation='vertical', fontsize=17)
    plt.yticks(fontsize=20)
    base_dir = os.getcwd()
    base_name = os.path.basename(preds_f).split('.')[0]
    ofn = os.path.join(base_dir, base_name + '.png')
    plt.savefig(ofn)
    plt.show()
    return