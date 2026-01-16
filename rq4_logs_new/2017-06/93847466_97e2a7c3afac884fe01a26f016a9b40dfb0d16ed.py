#!/usr/bin/env python3
# -*- coding: utf-8 -*-

####################################################################################################
#
# Script to evaluate the classification performance of the different label methods in labelGenes.py
# Uses RF and 10-fold crossvalidation
#
####################################################################################################

import os
import matplotlib.pyplot as plt

#For each label method run the label script and the classification ones
methods=["median","mean","zero"]
classificationMethods=["RF","SVM"]
for m in methods:
    print(m)
    os.system("python preprocessing/labelGenes.py -a ~/Desktop/FirstExampleData/ENCFF937GNL.tsv "+
              "-b ~/Desktop/FirstExampleData/ENCFF047WAI.tsv -m "+ m + " -o ~/Desktop/testLabels.txt "+
              " --protCod -g ~/Desktop/FirstExampleData/gencode.v26.annotation.gtf")
    
    for cM in classificationMethods:
        print(cM)
        os.system("python methods/classification.py -i ~/Desktop/input_mRNA.txt -l ~/Desktop/testLabels.txt "+ 
                  "-c 10 -a -o ~/Desktop/evalLabels.txt -n -m "+cM)

#Get the scores from the result file
#get the calculated score values from the file
scores=[]
fileLabel = open("/home/sch/schmidka/Desktop/evalLabels.txt")
for line in fileLabel.readlines():
	lineSplit=line.split()
	scores.append(list(map(float,lineSplit[2:])))
    
#Plot the results
plt.boxplot(scores)
labels=[a+" "+b for a in methods for b in classificationMethods]
plt.xticks(range(1,len(labels)+1),labels)
plt.xlabel("Labeling method")
plt.ylabel("AUC score")
plt.title('AUC Score of RF for different labeled sets')
plt.savefig('/home/sch/schmidka/Desktop/evalLabels.png')
plt.show()
                                                                                         