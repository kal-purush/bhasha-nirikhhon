# -*- coding: utf-8 -*-
"""
Created on Sat Apr 21 13:13:14 2018

@author: Lenovo
"""
import numpy as np

def Vec_Sim(Base, target, K):
    target = target[0]
    Top_K = [(0,0) for i in range(0,K)]
    for i in range(0,len(Base)):
        Vec = Base[i]
#        Vec = np.array([Base[i]])
#        print(np.array([Base[i]]).shape)    
        Sim = np.dot(Vec,target)/(np.linalg.norm(Vec)*(np.linalg.norm(target)))
#        print(Sim)
#        break
        if Sim > Top_K[-1][1]:
            for j in range(K-1,-1,-1):
                if j == 0:
                    if Sim > Top_K[0][1]:
                        Top_K.insert(0,(i,Sim))
                    elif Sim > Top_K[1][1]:
                        Top_K.insert(1,(i,Sim))
                else:
                    if Top_K[j][1] <= Sim:
                        continue
                    elif (i,Sim) not in Top_K:
                        Top_K.insert(j+1,(i,Sim))
                        
                if len(Top_K) > K:
                    Top_K = Top_K[:K]
        
    return Top_K
            
# BaseFile = "D:/AU_STUDY/9900/Compressed_Whole_vector.csv"
# Base = np.loadtxt(open(BaseFile),delimiter=",",skiprows=0)
# #print(Base[0])
# targetFile = "D:/AU_STUDY/9900/Compressed_test_vector.csv"
# target = np.loadtxt(open(targetFile),delimiter=",",skiprows=0)
# #target = np.array([target])
# #print(target)
# K = 5
# Top_K =  Vec_Sim(Base[:200], target, K)
# for pair in Top_K:
#     print(str(pair[0])+": "+str(pair[1]))