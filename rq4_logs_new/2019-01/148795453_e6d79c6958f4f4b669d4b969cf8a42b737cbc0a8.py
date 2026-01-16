#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  9 14:02:45 2019

@author: janik
"""

#############EXERCICE 1

def exercice1(d,c):
    if(c>0 and type(c) == int and d>0 and type(d) == int):
        l=[]
        for x in range(c):
            for y in range(c):
                if(x**2 + d*y**2 == c):
                    l.append((x,y))
        return(l)
    elif(c>0 and d>0 and(type(c) != int or type(d) != int)):
        print("c et d doit étre entier")
    elif((c<=0 or d<=0) and type(c) == int and type(d) == int):
        print("c et d doit étre positifs")
    else:
        print("Bad numbers")
        