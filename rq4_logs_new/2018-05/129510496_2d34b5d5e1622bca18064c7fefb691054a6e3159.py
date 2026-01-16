# -*- coding: utf-8 -*-
"""
Created on Tue May 22 03:16:34 2018

@author: Raheel
"""
import json
import frequency as f
import handler as handler
import googledirections as gd


json_data=open('data.txt').read()

data2 = json.loads(json_data)

if (data2[2]!='1'):
    user=1
    y=f.addPointsToDict('1')
    
if (data2[2]=='1'):
    user=1
    y=f.addPointsToDict('1')

first_lat,first_lng=data2[0].split(",")

second_lat,second_lng=data2[1].split(",")

directions = gd.googledirections(first_lat,first_lng,second_lat,second_lng)

centroids=[]

ProbabilityOFAllRoutes, RouteToPrint, index_max, centroids= handler.Probability2(directions,y)

with open('response.txt', 'w') as outfile:
   json.dump(RouteToPrint[index_max], outfile)
    

