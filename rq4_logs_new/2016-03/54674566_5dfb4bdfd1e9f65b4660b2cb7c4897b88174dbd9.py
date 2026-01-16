# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BIOL7800 assignment 16
Oscar Johnson 24 March 2016

Copyright Oscar Johnson 2016
"""

import matplotlib.pyplot as plt
import numpy as np


def listify(file):
    """
    takes input .csv file and splits on commas
    returns list of lists, pertaining to columns 
    first item in each list is the column name
    """
    my_lists = []
    for line in file:
        li = line.replace('\n', '')
        l = li.split(',')
        my_lists.append(l)
    return my_lists


def array(my_list):
    """convert list to numpy array"""
    array = np.array(my_list)
    return array


def main():
    my_file = open('/Users/home/LA_Tissues/LA_month_tissue_DB.csv', 'r')
    lists = listify(my_file)
    my_array = array(lists)
    print(my_array[:,0])
    for line in lists:
        print(line)

if __name__ == '__main__':
    main()