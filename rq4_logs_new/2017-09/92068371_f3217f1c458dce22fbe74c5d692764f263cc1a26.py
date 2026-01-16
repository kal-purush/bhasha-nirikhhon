import csv
import sys, os
import glob
import matplotlib.pyplot as plt
import numpy as np
from sklearn import metrics

os.chdir("c:\\try") #directory


#load lengths file. format: original lengths - predicted lengths (all bianary values of 1,0)
with open ("predict.csv", "rb") as readS:
        reader_unit = csv.reader(readS)
        unit = list(reader_unit)


original = [row[0] for row in unit] 
original = [int(i) for i in original] # char to int array
predicted =[row[1] for row in unit]
predicted = [int(i) for i in predicted] #char to int array



#roc curve parameters
fpr, tpr, thresholds = metrics.roc_curve(original, predicted, pos_label=1)


# This is the ROC curve
plt.plot(fpr,tpr)
plt.show() 

# This is the AUC - area under curve 
auc = np.trapz(tpr,fpr)
print auc 


#write the roc parameters to file if needed
with open ("rocData.csv", "wb") as writeU:
    writer = csv.writer(writeU)
    #for aa in result:
     #   print aa
    writer.writerow(thresholds)
    writer.writerow(fpr)
    writer.writerow(tpr)
writeU.close()
