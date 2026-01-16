#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  7 22:19:05 2018

@author: tomer
"""

#%%
# =================================================
# # T-test for the average BMI of mutaated VS wt
# =================================================

import numpy as np
import pandas as pd
import scipy.stats as st
from itertools import islice

#%%

gene_bmi_mut = pd.read_table('./../databases/mutation_bmi/UCEC_bmi_gene_mut.txt', sep = '\t', index_col = 0)

p_value = []
wt_avg = []
mut_avg = []
bmi = gene_bmi_mut.loc['BMI'][:-1]
for index, row in islice(gene_bmi_mut.iterrows(), 2, None):
    mut = row[:-1]
    wt_bmi = bmi[mut == 0].values
    mut_bmi = bmi[mut == 1].values
    wt_avg.append(wt_bmi.mean())
    mut_avg.append(mut_bmi.mean())
    p_value.append(st.ttest_ind(wt_bmi, mut_bmi).pvalue)


#%%

gene_bmi_mut['WT_AVG'] = ['-','-'] + wt_avg
gene_bmi_mut['MUT_AVG'] = ['-','-'] + mut_avg
gene_bmi_mut['P_VALUE'] = ['-','-'] + p_value
gene_bmi_mut = gene_bmi_mut.sort_values(by='P_VALUE')
gene_bmi_mut.to_csv('./../output/bmi_mut_p_value.txt', sep = '\t')