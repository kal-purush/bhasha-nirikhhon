import numpy as np
import matplotlib as mpl
import glob, os
import warnings
from matplotlib import pyplot as plt
from scipy.stats import skew, kurtosis
import math
import random
import pandas as pd
import sys
from scipy import stats

warnings.filterwarnings("ignore", category=mpl.cbook.mplDeprecation)
warnings.filterwarnings("ignore", category=RuntimeWarning)


# Creates dataframe for file
def DF(path):
    axislabels = ["DM", "Time", "S/N", "Width"]
    Tfile = open(path,'r')
    data = np.fromfile(Tfile,np.float32,-1) 
    c = data.reshape((-1,4))
    df = pd.DataFrame(c,columns=axislabels)
    Tfile.close()
    return df

def progressBar(value, endvalue, bar_length=20):
    
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    
    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))

# Method to calculate and return the theoretical DM range span given a certain
# time duration/width and peak magnitude
def cordes(Wms,SNratio):
    freq = 0.334
    bandWidth = 64

    x = np.linspace(-1000,1000,10000)   # Generates x-values for the cordes function
    zeta = (6.91*10**-3)*bandWidth*(freq**-3)*(Wms**-1)*x       # Zeta function in the cordes function
    
    first = 0       # Variable indicating whether bottom or top of DM range has been found
    bot_dm = 0      # Value of the lower end of the DM range
    top_dm = 0      # Value of the upper end of the DM range
    for i in range(len(x)): # Loops through the x-values and calculates the cordes value in each step
        y = (math.pi**(1/2))*0.5*(zeta[i]**-1)*math.erf(zeta[i])
        if (y >= SNratio) and (first == 0): # First time the theoretical ratio goes above the actual ratio go in here
            bot_dm = x[i]                   # This x-value corresponds to the bottom DM value
            first = 1                       # Changes variable value to not go into this statement again
        if (y <= SNratio) and (first == 1): # First time the theoretical ratio goes below the actual ratio after bottom is found
            top_dm = x[i]                   # This x-value corresponds to the top DM value
            break                           # Values have been found, break out of loop
    dm_range = top_dm - bot_dm              # Theoretical allowed DM range for the current candidate
    return dm_range
    
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

intention = input("Generate candidate bursts only (c), or generate training set (t)? ")

while intention != "c" and intention != "t":
    print("Invalid input, please indicated by entering either 'c'" + " or 't'.")
    intention = input("Generate candidate bursts only (c), or generate training set (t)? ")

numBursts = input("Enter number of candidates to simulate: ")

while True:
    try:
        numBursts = int(numBursts)
        break
    except:
        print("Invalid input, please indicate choice using an integer.")
        numBursts = input("Enter number of candidates to simulate: ")

if intention == "g" or intention == "t":
    folderName = input("Name of folder to be created: ")
    while folderName == "":
        print("Please enter a name of the folder to be created: ")
        folderName = input("Name of folder to be created: ")
    while True:
        try:    # Only creates folders if they don't already exist
            os.mkdir(os.getcwd() + '\odir\\' + folderName)
            break
        except:
            print("A folder with this name already exists. Please enter a different name.")
            folderName = input("Name of folder to be created: ")


freq = 0.334                    # F0 of survey
bandWidth = 64                  # Survey bandwidth

dmRangeBot = 100                # Lower possible DM for burst 
dmRangeTop = 650                # Upper possible DM for burst
dmWidthBot = 0                  # Lower possible DM width of a burst
snRangeBot = 10                 # Lower possible peak SN for burst
snRangeTop = 40                 # Upper possible peak SN for burst
quantFrac = 0.8                 # Fraction over which the quantile method to calculate duration is used
fraction = 0.75                 # Probability of there being a point in a single DM step
noiseFraction = 0.1             # Fraction of noise events in a candidate

bursts = []                     # All bursts are stored here, array of arrays
counter = 0


source_paths = []   # Array of file paths to be reviewed
shape_vals = []     # Array containing the shape feature values of the candidates
skew_vals = []      # Array containing the skewness feature values of the candidates
kurt_vals = []      # Array containing the kurtosis feature values of the candidates
kstest_vals = []    # Array containing the ks-test feature values of the candidates
class_vals = []     # Array containing the classification labels of the candidates

# Loops through all .dat files to store them in the 'source_paths' array
for file in glob.glob(os.getcwd() + '\idir\\' + "*.dat"):
    source_paths.append(file)

print("\nGenerating candidates")

if intention == "t":
    typeVar = 0
else:
    typeVar = 0.5

while counter < numBursts:
    progressBar(counter, numBursts)
    counter += 1
    
    upperTime = 50
    
    snArr = []              # All SN values of a burst
    dmArr = []              # All DM value of a burst
    tArr = []               # All times for the burst
    wArr = []
    labArr = []
    timeMid = np.random.uniform(1,upperTime)                # Average time of detection for a burst
    dmMid = np.random.uniform(dmRangeBot, dmRangeTop)       # Average DM of the burst
    peakSN = np.random.uniform(snRangeBot, snRangeTop)      # Peak SN of the burst
    SNratio = np.random.uniform(0.05,0.25)
    
    dmWidthTop = 0.2*dmMid
    
    # DM width of a burst
    """
    dmWidth = 0
    while dmWidth <= 20:
        dmWidth = np.random.normal((dmWidthTop - dmWidthBot)/2,(dmWidthTop - dmWidthBot)/4)
    # Time duration of the burst"""
    Wms = 0
    while (Wms < 1):
        Wms = (np.random.gumbel(0.25,0.09)*80)**1.5     # Draws duration from this distribution
    timeRange = Wms/(2*quantFrac)       # Time range for the time distribution of the burst, centered around 0
    
    dmWidth = cordes(Wms,SNratio)
    
    numPoints = 0       # Number of points in the burst
    while numPoints < 20:
        numPoints = int(np.random.gumbel(0.015, 0.06)*2000) # Draws the number of points from this distribution
    
    
    stDevSN = 2             # Standard deviation of the SN of the burst events
    stDevDM = dmWidth*0.1   # Standard deviation of the DM of the burst events
    start = -dmWidth/2          # Lower end of the DM range of the burst, currently centered around 0
    step = dmWidth/numPoints    # Size of each DM step when looping through the DM range
    count = 0                   # Countiong the number of events in the burst 
    
    # Loops through here to generate all possible points for the burst
    for i in range(numPoints + 1):
        tempVar1 = random.uniform(0,1)      # Randomises a number to determine whether a point should be generated or not
        if tempVar1 >= fraction:    # Does not generate a point
            continue
        else:                       # Does generate a point
            count += 1
            dmTemp = start + step*i     # DM for the point to be generated, relative to the DM centre of the burst
            if dmTemp == 0:     # Sorts the dm = 0 issue for the zeta function
                dmTemp = 0.00000001
        
            devSN = np.random.normal(0,stDevSN)   # Deviation from the theoretical SN value for the current point
            devDM = np.random.normal(0,stDevDM)   # Deviation from the DM value for the current point
            dmArr.append(dmMid + dmTemp + devDM)    # Adds the actual DM value of the point to an array

            zeta = (6.91*10**-3)*bandWidth*(freq**-3)*(Wms**-1)*(dmTemp + devDM)        # Zeta-function
            snArr.append(math.pi**(1/2)*0.5*(zeta**-1)*math.erf(zeta)*(peakSN + devSN)) # SN value of the point, including deviation
            
            timeVar = timeMid + np.random.uniform(-timeRange,timeRange)/1000    # Time of detection of the points
            tArr.append(timeVar)    # Adds the time to the time array
            wArr.append(32)
            labArr.append(1)
        
            if tempVar1 <= noiseFraction*fraction:  # Generates the the noise points
                count += 1
                noiseDM = np.random.uniform((dmMid + start*1.1 - stDevDM*3), (dmMid + stDevDM*3 - start*1.1))
                noiseSN = np.random.uniform(0, peakSN*1.1)
                noiseTime = timeMid + np.random.uniform(-timeRange,timeRange)/1000
                
                dmArr.append(noiseDM)
                snArr.append(noiseSN)
                tArr.append(noiseTime)
                wArr.append(32)
                labArr.append(0)
                
        
    # Creates a numpy table of the burst, same format as the standard .dat files
    totArr = np.zeros([count, 5])
    totArr[:,0] = np.array(np.transpose(dmArr))
    totArr[:,1] = np.array(np.transpose(tArr))
    totArr[:,2] = np.array(np.transpose(snArr))
    totArr[:,3] = np.array(np.transpose(wArr))
    totArr[:,4] = np.array(np.transpose(labArr))
    
    bursts.append(totArr)

progressBar(numBursts,numBursts)               
counter = 0
print("\nGenerating non-candidates")

while (counter < numBursts) and (intention == "t"):
    progressBar(counter, numBursts)
    counter += 1
    counterFactor = 1
    
    upperTime = 50
    
    snArr = []              # All SN values of a burst
    dmArr = []              # All DM value of a burst
    tArr = []               # All times for the burst
    wArr = []
    labArr = []
    timeMid = np.random.uniform(1,upperTime)                # Average time of detection for a burst
    dmMid = np.random.uniform(dmRangeBot, dmRangeTop)       # Average DM of the burst
    peakSN = np.random.uniform(snRangeBot, snRangeTop)      # Peak SN of the burst
    
    dmWidthTop = 0.2*dmMid
    
    # DM width of a burst
    dmWidth = 0
    while dmWidth <= 20:
        dmWidth = np.random.normal((dmWidthTop - dmWidthBot)/2,(dmWidthTop - dmWidthBot)/4)
    # Time duration of the burst
    Wms = 0
    while (Wms < 1):
        Wms = (np.random.gumbel(0.25,0.09)*80)**1.5     # Draws duration from this distribution
    timeRange = Wms/(2*quantFrac)       # Time range for the time distribution of the burst, centered around 0
    
    numPoints = 0       # Number of points in the burst
    while numPoints < 20:
        numPoints = int(np.random.gumbel(0.015, 0.06)*2000) # Draws the number of points from this distribution
    
    
    stDevSN = np.random.uniform(2,5)             # Standard deviation of the SN of the burst events
    stDevDM = dmWidth*np.random.uniform(0.1,0.4)   # Standard deviation of the DM of the burst events
    start = -dmWidth/2          # Lower end of the DM range of the burst, currently centered around 0
    step = dmWidth/numPoints    # Size of each DM step when looping through the DM range
    count = 0                   # Countiong the number of events in the burst 
    

    typeVar = np.random.uniform(0,1)
    typeVar = 0.2
    side = np.random.uniform(0,1)
    botSN = np.random.uniform(0,peakSN-1)
    horSN = np.random.uniform(9,15)
    horBotSN = np.random.uniform(9,14)
    horStDevSN = np.random.uniform(0,1)
    count = 0
    pointNum = 0
    horLinOrd = np.random.uniform(0,1)
    
    # Loops through here to generate all possible points for the burst
    for i in range(numPoints + 1):
        tempVar1 = random.uniform(0,1)      # Randomises a number to determine whether a point should be generated or not
        if tempVar1 >= fraction:    # Does not generate a point
            continue
        else:                       # Does generate a point
            count += 1
            dmTemp = start + step*i     # DM for the point to be generated, relative to the DM centre of the burst
            if dmTemp == 0:     # Sorts the dm = 0 issue for the zeta function
                dmTemp = 0.00000001
        
            devSN = np.random.normal(0,stDevSN)   # Deviation from the theoretical SN value for the current point
            devDM = np.random.normal(0,stDevDM)   # Deviation from the DM value for the current point

            if typeVar >= 0.75:     # Linear SN shape
                if side >= 0.5:
                    c = ((peakSN + botSN) - ((peakSN - botSN)/(dmWidth))*(2*dmMid))/2
                    k = (peakSN - botSN)/(dmWidth)
                    y = (dmMid + dmTemp)*k + c
                    snArr.append(y) # SN value of the point, including deviation
                else:
                    c = (botSN + peakSN - ((botSN - peakSN)/(dmWidth))*(2*dmMid))/2
                    k = (botSN - peakSN)/(dmWidth)
                    y = (dmMid + dmTemp)*k + c
                    snArr.append(y) # SN value of the point, including deviation
                dmArr.append(dmMid + dmTemp + devDM)
                    
            elif typeVar >= 0.5:    # Horizontal SN shape
                devSN = np.random.normal(0,horStDevSN)  # Using own StDev since these tend to be narrower than normal signals
                y = horSN + devSN
                snArr.append(y)
                
            elif typeVar >= 0.25:   # Random uniform SN distribution
                y = horSN + devSN
                snArr.append(y)
                
            else:
                pointNum += 1
                if horLinOrd >= 0.5:
                    if pointNum >= numPoints/2:
                        altWidth = dmWidth/2
                        devDM = np.random.normal(0,4)
                        if side >= 0.5:
                            c = (horSN + horBotSN - ((horSN - horBotSN)/(altWidth))*(2*(dmMid + altWidth)))/2
                            k = (horSN - horBotSN)/(altWidth)
                            y1 = ((dmMid + altWidth) + dmTemp)*k + c
                            snArr.append(y1) # SN value of the point, including deviation
                        else:
                            c = (horBotSN + horSN - ((horBotSN - horSN)/(altWidth))*(2*(dmMid + altWidth)))/2
                            k = (horBotSN - horSN)/(altWidth)
                            y1 = ((dmMid + altWidth) + dmTemp)*k + c
                            snArr.append(y1) # SN value of the point, including deviation
                    else:
                        devSN2 = np.random.normal(0,horStDevSN)  # Using own StDev since these tend to be narrower than normal signals
                        y2 = horSN + devSN2
                        snArr.append(y2)
                else:
                    if pointNum <= numPoints/2:
                        altWidth = dmWidth/2
                        devDM = np.random.normal(0,4)
                        if side >= 0.5:
                            c = (horSN + horBotSN - ((horSN - horBotSN)/(altWidth))*(2*(dmMid + altWidth)))/2
                            k = (horSN - horBotSN)/(altWidth)
                            y1 = ((dmMid + altWidth) + dmTemp)*k + c
                            snArr.append(y1) # SN value of the point, including deviation
                        else:
                            c = (horBotSN + horSN - ((horBotSN - horSN)/(altWidth))*(2*(dmMid + altWidth)))/2
                            k = (horBotSN - horSN)/(altWidth)
                            y1 = ((dmMid + altWidth) + dmTemp)*k + c
                            snArr.append(y1) # SN value of the point, including deviation
                    else:
                        devSN2 = np.random.normal(0,horStDevSN)  # Using own StDev since these tend to be narrower than normal signals
                        y2 = horSN + devSN2
                        snArr.append(y2)
                dmArr.append(dmMid + dmTemp + devDM)
                    
            timeVar = timeMid + np.random.uniform(-timeRange,timeRange)/1000    # Time of detection of the points
            tArr.append(timeVar)    # Adds the time to the time array
            wArr.append(32)
            labArr.append(0)

    # Creates a numpy table of the burst, same format as the standard .dat files
    totArr = np.zeros([count, 5])
    totArr[:,0] = np.array(np.transpose(dmArr))
    totArr[:,1] = np.array(np.transpose(tArr))
    totArr[:,2] = np.array(np.transpose(snArr))
    totArr[:,3] = np.array(np.transpose(wArr))
    totArr[:,4] = np.array(np.transpose(labArr))
    
    bursts.append(totArr)    

bursts = np.array(bursts)
progressBar(numBursts,numBursts)              
counter = 0
print("\nCalculating features")

for q in range(len(bursts)):
    progressBar(q,len(bursts))
    
    signalToDm = list(zip(bursts[q][:,0], bursts[q][:,2]))
    signalToDm = np.array(signalToDm)
    
    shifted = list(zip(bursts[q][:,0], bursts[q][:,2]))
    shifted = np.array(shifted)
    min_val = min(shifted[:,1])
    
    """
    # Sets y = 0 for visualisation
    for i in range(len(signalToDm[:,1])):
        signalToDm[:,1][i] = signalToDm[:,1][i] - min_val"""

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
    xwidth=(min(signalToDm[:,0]) - max(signalToDm[:,0]))/12        

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
    ks_stat = ks_cordes(signalToDm[:,0],signalToDm[:,1],bursts[q][:,1],meanDM[max_ind])     # KS feature
    
    # Adds the feature values to the corresponding arrays
    shape_vals.append(shape_conf)
    skew_vals.append(skewness)
    kurt_vals.append(kurt)
    kstest_vals.append(ks_stat)
    class_vals.append(bursts[q][0][4])
    
# Creates dataframe table containing all feature values as well as the classification labels for each cluster
dataframe = pd.DataFrame({'Shape Feature': shape_vals,
                          'Skewness': skew_vals,
                          'Kurtosis': kurt_vals,
                          'KS-test stat': kstest_vals,
                          'Label': class_vals})

dataframe.to_csv(os.getcwd() + '\\odir\\' + folderName + "\\" + "feature_table.csv", index = False)   # Writes dataframe to .csv file
progressBar(numBursts,numBursts) 