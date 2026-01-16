# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 09:21:15 2020

@author: user
"""

import os.path

if os.path.isfile("myfile.txt"):
    print("file is here")
else:
    print("file is not here")

fo=open('myfile.txt',"w")
fo.write(" Kyle ")

fo=open('myfile.txt',"r")
fo.read()
print(fo)

fo=open('myfile.txt',"a")
fo.write("is me")

fo.close()