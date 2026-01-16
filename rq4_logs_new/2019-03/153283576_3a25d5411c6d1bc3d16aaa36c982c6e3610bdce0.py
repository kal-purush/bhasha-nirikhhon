#import pandas as pd
import sys
import numpy as np
from scipy.stats import skew, kurtosis
from scipy import stats
#import os
import math


def progressBar(value, endvalue, bar_length=20):
    
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    
    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    
    
# Conducts the 2d KS-test on the SN-DM distribution and the theoretical cordes equation
def ks_cordes(dmArr,snArr,timeArr,peakDmMean):
    freq = 0.334
    bandWidth = 64
    cordes = []             # y-values of the theoretical cordes function
    peakSN = max(snArr)     # Value of the higher SN-bin of the data
    snFreqArr = []          # Probability frequency distribution for the data
    cordesFreqArr = []      # Probability frequency distribution for the theoretical function
    
    Wms = np.percentile(timeArr,75)-np.percentile(timeArr,25)   # Time width using the quantile method
    Wms = Wms*1000                                              # Must be in milliseconds
    dmScaled = dmArr - peakDmMean                               # Centers the data around DM = 0
    snRatios = (snArr + 9)/(peakSN + 9)                         # Ratios of the SN-values in relation to the peak
    
    x = np.linspace(min(dmScaled),max(dmScaled),2000)           # X-values for cordes function
    zeta = (6.91*10**-3)*bandWidth*(freq**-3)*(Wms**-1)*x       # Zeta function, see Cordes & M
    zeta[zeta == 0] = 0.000001
    
    
    # Calculates the y-values of the theoretical function
    for i in range(len(x)):
        cordes.append((math.pi**(1/2))*0.5*(zeta[i]**-1)*math.erf(zeta[i]))

    altArr = []

    # Creates prob. freq. representation of the SN distribution
    for i in range(len(snRatios)):
        temp_arr = []
        frequency = int(snRatios[i]*100)      # Needs to be an integer, timed it by 1000 to reduce rounding errors, proportions still same
        altArr.append(frequency)
        temp_arr = [dmScaled[i]] * frequency   # Creates the corresponding number of elements and adds it to the array
        snFreqArr.extend(temp_arr)

    # Creates prob. freq. representation of the cordes func.
    for i in range(len(cordes)):
        temp_arr = []
        frequency = int(cordes[i]*100)
        temp_arr = [x[i]] * frequency
        cordesFreqArr.extend(temp_arr)
        
    statistic = stats.ks_2samp(snFreqArr,cordesFreqArr) #2D KS-test
    return statistic[0]



def featureFile(burstsArr):
    
    shape_vals = []
    skew_vals = []
    kurt_vals = []
    kstest_vals = []
    class_vals = []
    #numBursts = len(burstsArr)
    
    for q in range(len(burstsArr)):
        progressBar(q,len(burstsArr))
        
        signalToDm = list(zip(burstsArr[q][:,0], burstsArr[q][:,2]))
        signalToDm = np.array(signalToDm)
        
        shifted = list(zip(burstsArr[q][:,0], burstsArr[q][:,2]))
        shifted = np.array(shifted)
        
        # Splitting into chunks of equal number of events in each
        split_param = 7 # Number of chunks to be split into
        dummy = np.array_split(shifted,split_param)
        
        meanSN = []     # Contains mean SN value of each chunk 
        meanDM = []     # Contains mean DM value of each chunk
        
        # Loops through the chunks, calculates the relevant mean values and puts them into the appropriate arrays
        for i in range(len(dummy)):
            tempSN = np.mean(dummy[i][:,1])
            tempDM = np.mean(dummy[i][:,0])
            meanSN.append(tempSN)
            meanDM.append(tempDM)
            
        max_ind = np.argmax(meanSN)     # Finds the index for the highest SN bin value
        
        freq_arr = []       # Probability frequency distribution representation of the DM - SN data
        
        weight_1 = 0.3      # Score weight if ratio is less than 1 but more than 1 - check_1
        weight_2 = -0.3     # Score weight if ratio is less than 1 - check_1, but more than 1 - check_2
        weight_3 = 1        # Score weight if ratio is less than 1 - check_2
        weight_4 = -1       # Score weight if ratio is more than 1
        check_1 = 0.075
        check_2 = 0.15
        score = [0,1.3,2.5,2.5]                         # Scoring system where one index step corresponds to one step from peak bin
        max_score = 2*(score[0] + score[1] + score[2])  # Maximum possible score
        rating = 0  # Rating score after weight and scores have been applied
        
        if (max_ind > 4) or (max_ind < 2):
            rating = 0
        else:
            for i in range(max_ind - 1, -1, -1):                # Loops through all bins from the peak bin moving to the left
                ratio=meanSN[i]/meanSN[i+1]                     # Ratio of the next bin to the previous bin
            
                if ((ratio>=(1-check_1)) and (ratio<=1)):
                    rating += weight_1*score[max_ind-(i+1)]
                elif ((ratio>=(1-check_2)) and (ratio<=1)):
                    rating += weight_2*score[max_ind-(i+1)]
                elif (ratio<=1):
                    rating += weight_3*score[max_ind-(i+1)]
                else:
                    rating += weight_4*score[max_ind-(i+1)]
                            
            for i in range((max_ind+1),split_param):            # Loops through all bins from the peak bin moving to the right
                ratio=meanSN[i]/meanSN[i-1]
        
                if ((ratio>=(1-check_1)) and (ratio<=1)):
                    rating += weight_1*score[i-max_ind-1]
                elif ((ratio>=(1-check_2)) and (ratio<=1)):
                    rating += weight_2*score[i-max_ind-1]
                elif ratio <=1:
                    rating += weight_3*score[i-max_ind-1]
                else:
                    rating += weight_4*score[i-max_ind-1]    
        
        # Exception case where rating is less than 0, sets rating to 0 if this happens
        if rating < 0:
            rating = 0
        
        # Converts the S/N-DM plot into a probability frequency plot
        # Instead of each point in DM space having a corresponding S/N y-value
        # there will be an array containing a number of DM elements proportional to its S/N value
        normal_snRatios = (signalToDm[:,1])/(max(signalToDm[:,1]))
        for i in range(len(signalToDm)):
            temp_arr = []
            frequency = int((normal_snRatios[i])*1000)      # Needs to be an integer, timed it by 1000 to reduce rounding errors, proportions still same
            temp_arr = [signalToDm[i][0]] * frequency   # Creates the corresponding number of elements and adds it to the array
            freq_arr.extend(temp_arr)
        
        # FEATURES
        shape_conf = rating/max_score                       # Shape conf feature
        skewness = skew(freq_arr, axis = 0)                 # Skewness feature
        kurt = kurtosis(freq_arr, axis = 0, fisher = True)  # Kurtosis feature
        ks_stat = ks_cordes(signalToDm[:,0],signalToDm[:,1],burstsArr[q][:,1],meanDM[max_ind])     # KS feature
        
        # Adds the feature values to the corresponding arrays
        shape_vals.append(shape_conf)
        skew_vals.append(skewness)
        kurt_vals.append(kurt)
        kstest_vals.append(ks_stat)
        class_vals.append(burstsArr[q][0][4])
    
    return shape_vals, skew_vals, kurt_vals, kstest_vals, class_vals




    