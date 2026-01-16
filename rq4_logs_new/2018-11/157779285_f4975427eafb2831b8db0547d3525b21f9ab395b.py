#!/usr/bin/env python
# coding: utf-8

# # EDA on Breast Cancer Data from Kaggle
# ***
# quick_csv_stat.py
# Milos Milic Arzan Irani, Nov 2018
#
# This is a script that shows our initial EDA to test for two things
# It tests for any NAs or Null values in our data set.
#
# Then we only keep at the features we want to use in our training and data set in
# in order to remove redundancy.
#
# The script takes a variable from a .csv file. .
# Then we explore differences beween the features based on our target feature
# which is diagnosis.
# We then save the data into a new file
# clean_breast_cancer_data_after_dropped_features.csv
#
# Dependencies: argparse, pandas, numpy, seaborn, matplotlib
#
# Usage: 01_read_data.py, clean_breast_cancer_data

# libraries I am adding
import argparse

import pandas as pd
import matplotlib.pyplot as plt
#%matplotlib inline
import numpy as np
import seaborn as sns


# We begin by reading the data in and making some visual observations about the same

# In[11]:

def main():

# Reading in the data in from the CSV in the data folder using pandas
    breast_cancer_data = pd.read_csv("../data/clean_breast_cancer_data.csv")

#Testing out the number of nulls in any of the columns
    summary_of_nulls = pd.DataFrame(columns=['column_name', 'number_of_nulls'])
    number_of_nulls = []

    col_names = breast_cancer_data.columns.tolist()

    for cols in col_names:
        number_of_nulls.append(breast_cancer_data[cols].isnull().sum())

    summary_of_nulls['number_of_nulls'] = number_of_nulls
    summary_of_nulls['column_name'] = col_names

    malignant_breast_cancer_data = breast_cancer_data.query('diagnosis == "M"')
    benign_breast_cancer_data = breast_cancer_data.query('diagnosis == "B"')

    # Let's only use the frst 12 columns
    malignant_breast_cancer_data = malignant_breast_cancer_data.iloc[:,0:12]
    benign_breast_cancer_data = benign_breast_cancer_data.iloc[:,0:12]

    malignant_breast_cancer_data.head()

    feature_col_names = malignant_breast_cancer_data.columns.tolist()[2:]
    feature_col_names_queue = feature_col_names.copy()

    plt.rcParams['figure.figsize']= 10,15
    fig, ax = plt.subplots(5, 2)
    fig.subplots_adjust(hspace=0.4, wspace=0.4)
    for row_num in range(ax.shape[0]):
        for col_num in range(ax.shape[1]):
            feature = feature_col_names_queue.pop(0)
            sns.distplot(malignant_breast_cancer_data[[feature]],
                         kde = False, ax = ax[row_num, col_num], color = 'red', label = 'malignant').set_title(feature)
            sns.distplot(benign_breast_cancer_data[[feature]],
                         kde = False, ax = ax[row_num, col_num], color = 'grey', label = 'benign')
            ax[row_num, col_num].legend()
    plt.savefig('../imgs/freature_histograms.png')


    features_to_explore_further = ['radius_mean', 'perimeter_mean',
                               'concavity_mean', 'texture_mean']

    plt.rcParams['figure.figsize']= 15,10
    fig2, ax2 = plt.subplots(2, 2)
    sns.set(style="ticks", palette="pastel")
    fig.subplots_adjust(hspace=0.4, wspace=0.3)
    for row_num in range(ax2.shape[0]):
        for col_num in range(ax2.shape[1]):
            feature = features_to_explore_further.pop(0)
            sns.boxplot(x = "diagnosis", y = feature,
                hue="diagnosis", palette=["red", "grey"], ax = ax2[row_num, col_num],
                data=breast_cancer_data)
    plt.savefig('../imgs/box_plots.png')


    breast_cancer_data = breast_cancer_data.iloc[:,0:12]
    breast_cancer_data
    breast_cancer_data.to_csv("../data/clean_breast_cancer_data_after_dropped_features.csv",encoding='utf-8', index=False)

main()