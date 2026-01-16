#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 19:16:36 2021

@author: richardlee
"""
import json
import tkinter
import tkinter.filedialog as tkFileDialog
import glob
import os

currdir = os.getcwd()
root = tkinter.Tk()
root.withdraw() #use to hide tkinter window
respInfoPath = tkFileDialog.askdirectory(parent=root, initialdir=currdir, title='Please select a directory')

'''
respInfoPath = '/Users/richardlee/Desktop/2021 Fall/Precision Care Medicine/Coding/PY16N008/ResponseInfo/CCEP'
'''

if len(respInfoPath) > 0:
    print ("You chose %s" % respInfoPath)

respInfoFile_path = glob.glob(respInfoPath + '/*.json', recursive=True)
respInfoFiles = os.listdir(respInfoPath)

stimCh1 = []
stimCh2 = []
for i in range(len(respInfoFiles)):
    txt = respInfoFiles[i]
    fileInfo = txt.split("_")
    stimCh1.append(fileInfo[1]);
    stimCh2.append(fileInfo[2]);

f = open(respInfoFile_path[1])
data = json.load(f)
f.close()

chNames = list(data['time'].keys())
