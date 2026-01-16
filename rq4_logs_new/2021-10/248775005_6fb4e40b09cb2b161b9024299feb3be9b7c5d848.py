# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 22:45:57 2020

@author: lenovo
"""
import re
import sys

def process(string):  # add after VHDL_RD
    res = ""
    if "after" in string:
        i = 0
        while i<len(string):
            if string[i]=="a" && string[i+1]=="f" && string[i+2]=="t" && string[i+3]=="e" && string[i]=="r":
                i += 5
    
    else:
        res += "(" + string + ")" + "after VHDL_RD"   
    return res


fpath = "F:/MyGit/leetcode/regex/"
file = open(fpath+"conv.v","r")
file2 = open(fpath+"res.v","w")
txts = file.read()

res = ""
i = 0
while i < len(txts)-1:
    if txts[i] == "<=":
        res += "<="
        j = i+1
        sub_str = ""
        while j!=";" :
            sub_str += txts[j]
            j += 1
        sub_str = process(sub_str)
        sub_str += ";"
        
        
    else:
        res += txts[i]
file2.write(txts)

file2.close()
    
    