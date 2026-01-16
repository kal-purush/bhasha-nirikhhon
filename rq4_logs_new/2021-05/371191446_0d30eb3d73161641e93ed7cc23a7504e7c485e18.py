import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from apyori import apriori
import xlrd #to read xls files
import xlwt # to write to a new xls file
from openpyxl import *  # to write to existing xlsx file
import csv
from numba.pycc.cc import CC
#store_data = pd.read_csv('/Users/bessghaiernarjess/Documents/workspace-luna/fault-inducing-commits/Co-occurrence/store_data.csv', header=None)
store_data = pd.read_csv('/Users/bessghaiernarjess/Documents/PhD_ETS/Contrib2-Fault-proneness/Occurrence/Sympfony.csv', header=None, low_memory=False)


#COMPUTE THE FREQUENCY OF SINGLE ITEMS
CC=0;
NPC=0;
EML=0;
ECL=0;
EPL=0;
TMPM=0;
TMM=0;
NoC=0;
DoI=0;
CBO=0;
ECB=0;
GTS=0;    
length=10717
cols=7
for i in range(0, length):
    for j in range(0, cols):
        if str(store_data.values[i,j])=="Cyclomatic Complexity":
            CC+=1
        if str(store_data.values[i,j])=="NPath Complexity":
            NPC+=1 
        if str(store_data.values[i,j])=="Excessive Method Length":
            EML+=1 
        if str(store_data.values[i,j])=="Excessive Class Length":
            ECL+=1  
        if str(store_data.values[i,j])=="Excessive Parameter List":
            EPL+=1
        if str(store_data.values[i,j])=="Too Many Public Methods":
            TMPM+=1 
        if str(store_data.values[i,j])=="Go to Statement":
            GTS+=1 
        if str(store_data.values[i,j])=="Too Many Methods":
            TMM+=1
        if str(store_data.values[i,j])=="Number Of Children":
            NoC+=1
        if str(store_data.values[i,j])=="Depth Of Inheritance":
            DoI+=1 
        if str(store_data.values[i,j])=="Coupling Between Objects":
            CBO+=1 
        if str(store_data.values[i,j])=="Empty Catch Block":
            ECB+=1                 

CC=CC/length;
NPC=NPC/length;
EML=EML/length;
ECL=ECL/length;
EPL=EPL/length;
TMPM=TMPM/length;
TMM=TMM/length;
NoC=NoC/length;
DoI=DoI/length;
CBO=CBO/length;
ECB=ECB/length;
GTS=GTS/length;  

records = []
for i in range(0, length):
    records.append([str(store_data.values[i,j]) for j in range(0, cols)])
    
    
association_rules = apriori(records, min_support=0.007, min_confidence=0.2, min_lift=4, min_length=2)
association_results = list(association_rules)

#print(len(association_results))
#print(association_results[0])

support=0
confidence=0
lift=0
leverage=0  #range -1-1
supportA=0
supportC=0
conviction=0
for item in association_results:

    # first index of the inner list
    # Contains base item and add item
    pair = item[0] 
    items = [x for x in pair]
    print("Rule: " + items[0] + " -> " + items[1])
    support=item[1]
    print("Support: " +str(support))
    confidence=item[2][0][2]
    print("Confidence: " + str(confidence))
    lift=item[2][0][3]
    print("Lift: " + str(lift))
    
    
    if items[0]=="Cyclomatic Complexity":
        supportA=CC
    if items[0]=="NPath Complexity":
        supportA=NPC 
    if items[0]=="Excessive Method Length":
        supportA=EML
    if items[0]=="Excessive Class Length":
        supportA=ECL  
    if items[0]=="Excessive Parameter List":
        supportA=EPL 
    if items[0]=="Too Many Public Methods":
        supportA=TMPM
    if items[0]=="Too Many Methods":
        supportA=TMM
    if items[0]=="Number Of Children":
        supportA=NoC 
    if items[0]=="Depth Of Inheritance":
        supportA=DoI
    if items[0]=="Coupling Between Objects":
        supportA=CBO
    if items[0]=="Empty Catch Block":
        supportA=ECB    
        
                
    if items[1]=="Cyclomatic Complexity":
        supportC=CC
    if items[1]=="NPath Complexity":
        supportC=NPC 
    if items[1]=="Excessive Method Length":
        supportC=EML
    if items[1]=="Excessive Class Length":
        supportC=ECL  
    if items[1]=="Excessive Parameter List":
        supportC=EPL 
    if items[1]=="Too Many Public Methods":
        supportC=TMPM
    if items[1]=="Too Many Methods":
        supportC=TMM
    if items[1]=="Number Of Children":
        supportC=NoC 
    if items[1]=="Depth Of Inheritance":
        supportC=DoI
    if items[1]=="Coupling Between Objects":
        supportC=CBO
    if items[1]=="Empty Catch Block":
        supportC=ECB  
     
                    
    leverage=support-(supportA*supportC)
    print("leverage: "+str(leverage)) 
    if(confidence==1):
        conviction=(1-supportC)/(confidence)
    else:
        conviction=(1-supportC)/(1-confidence)
    print("conviction: "+str(conviction))  
    print("=====================================")
    support=0
    confidence=0
    lift=0
    leverage=0
    conviction=0
    supportA=0
    supportC=0