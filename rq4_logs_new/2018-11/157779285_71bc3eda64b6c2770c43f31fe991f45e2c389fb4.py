#!/usr/bin/env python
# coding: utf-8


#  Hyperparameter Optimization
#
# ### My dependancies: Random pandas matplotlib.pyplot numpy seaborn
#  CountVectorizer, train_test_split
#  argparse

# Milos Milic Arzan Irani Nov 2018
#
# This script takes in the optimal depth and min sample split and plots a decision tree

#
# Usage: 03_hyperparmeter_optimization.py


import argparse
#from pdf2image import convert_from_path
import graphviz
from sklearn import tree
import random
import pandas as pd
import matplotlib.pyplot as plt
#get_ipython().run_line_magic('matplotlib', 'inline')
import numpy as np
import seaborn as sns
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split


# In[14]:

def main():
    # Reading in the data in from the CSV in the data folder using pandas
    breast_cancer_data = pd.read_csv("../data/clean_breast_cancer_data_after_dropped_features.csv")
    breast_cancer_data.head()


    # Let's fit the data to the model and draw it

    # In[15]:


    # Let's separate the features from the target
    features_X = breast_cancer_data.drop(columns = ['id','diagnosis'])
    target_y = breast_cancer_data[['diagnosis']]


    # In[16]:


    breast_cancer_malignancy_classifier = DecisionTreeClassifier(max_depth = 5, min_samples_split = 2)
    breast_cancer_malignancy_classifier.fit(features_X, target_y)
    breast_cancer_malignancy_classifier.score(features_X, target_y)


    # Final Model had training score of 0.98418

    # In[17]:


    feature_cols = breast_cancer_data.columns.tolist()[2:]
    feature_cols


    # In[18]:


    #help(graph.render)


    # In[19]:


    def save_and_show_decision_tree(model,
                                    class_names = ['Malignant', 'Benign'],
                                    save_file_prefix = 'breast_cancer_malignancy_tree', **kwargs):
        """
        Saves the decision tree model as a pdf and a
        """
        dot_data = tree.export_graphviz(model, out_file=None,
                                 feature_names=feature_cols,
                                 class_names=class_names,
                                 filled=True, rounded=True,
                                 special_characters=True, **kwargs)

        graph = graphviz.Source(dot_data)
        graph.render(save_file_prefix, directory = '../imgs/')
        return graph

    graph = save_and_show_decision_tree(breast_cancer_malignancy_classifier)
    

main()
    # In[ ]: