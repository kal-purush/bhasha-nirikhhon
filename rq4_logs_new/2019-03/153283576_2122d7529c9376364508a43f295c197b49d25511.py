import numpy as np
import pandas as pd
import matplotlib as mpl
import glob, os
import warnings
import math
import pickle
from scipy import stats
from sklearn.cluster import DBSCAN
from sklearn import preprocessing
from scipy.stats import skew, kurtosis
from timeit import default_timer as timer
from matplotlib import pyplot as plt

warnings.filterwarnings("ignore",category=mpl.cbook.mplDeprecation)
warnings.filterwarnings("ignore",category=RuntimeWarning)

# Returns 'num' lowest elements of array 'arr' in a new array
def sort(arr,num):
    xsorted = np.sort(arr)[:num]
    return xsorted

# Re-orders a shuffled labels_arr array so that the index of the array and the
# labels in the labels array has a q-1 relationship counting from q = 0.
# label -1 (q = 0) corresponds to RFI and everything above it is a legitimate cluster
def clusterOrder(clusterArr):

    lab_arr = np.unique(clusterArr)     # Creates an array containing each unique value of the cluster label array
    for q in range(1,len(lab_arr)):     # Loops through all the possible labels counting from 0 to the number of unique clusters
        clusterArr = np.where(clusterArr == lab_arr[q], q - 1, clusterArr)  

# Method to create an array of arrays where each index corresponds to a separate cluster
def clusterSort(clusterArr, pointsArr):
    temp_arr = []     # Array of arrays containing points for each label. e.g. labels_arr[0] contains all points with label -1
    lab_arr = np.unique(clusterArr)
    
    for i in range(len(lab_arr)):       # Creates the array of arrays to be used
        temp_list = []
        temp_arr.append(temp_list)
    
    for i in range(len(clusterArr)):    # Loops through all points in the cluster label array
        for q in range(len(lab_arr)):   # Loops through all indexes in the array of array
            if (clusterArr[i] == lab_arr[q]):       # Puts all points with the same label, from the cluster label arrays, in
                temp_arr[q].append(pointsArr[i])    # the same array at the same index
                break
    
    for q in range(len(temp_arr)):          # Makes all arrays Numpy arrays
        temp_arr[q] = np.array(temp_arr[q])
    
    temp_arr = np.array(temp_arr)           # Makes the array of arrays a Numpy array
    return temp_arr


# Creates dataframe for file
def DF(path):
    axislabels = ["DM", "Time", "S/N", "Width"]
    Tfile = open(path,'r')
    data = np.fromfile(Tfile,np.float32,-1) 
    c = data.reshape((-1,4))
    df = pd.DataFrame(c,columns=axislabels)
    Tfile.close()
    return df

# Conducts the 2d KS-test on the SN-DM distribution and the theoretical cordes equation
def ks_cordes(dmArr,snArr,timeArr,peakDmMean,freq = 0.334,bandWidth = 64):
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
    
    # Calculates the y-values of the theoretical function
    startx = timer()
    for i in range(len(x)):
        cordes.append((math.pi**(1/2))*0.5*(zeta[i]**-1)*math.erf(zeta[i]))
    endx = timer()
    cordesTimes.append(endx - startx)
    altArr = []

    startx = timer()
    # Creates prob. freq. representation of the SN distribution
    for i in range(len(snRatios)):
        temp_arr = []
        frequency = int(snRatios[i]*100)      # Needs to be an integer, timed it by 1000 to reduce rounding errors, proportions still same
        altArr.append(frequency)
        temp_arr = [dmScaled[i]] * frequency   # Creates the corresponding number of elements and adds it to the array
        snFreqArr.extend(temp_arr)
    endx = timer()
    ksDatProb.append(endx - startx)
    startx = timer()
    # Creates prob. freq. representation of the cordes func.
    for i in range(len(cordes)):
        temp_arr = []
        frequency = int(cordes[i]*100)
        temp_arr = [x[i]] * frequency
        cordesFreqArr.extend(temp_arr)
    endx = timer()
    ksTheProb.append(endx - startx)
    startx = timer()
    statistic = stats.ks_2samp(snFreqArr,cordesFreqArr) #2D KS-test
    endx = timer()
    KSstat.append(endx - startx)
    return statistic[0]

# Method to calculate and return the theoretical DM range span given a certain
# time duration/width and peak magnitude
def cordes(timeArr,peakSN,freq = 0.334,bandWidth = 64):
    cordes = []                 # Array containing all theoretical SN-values from the cordes function
    SNratio = 9/(peakSN + 9)    # Ratio of the peak to the cutoff point in the data
                                # Need to calculate the corresponding DM range for a reduction from 
                                # 1 (peak) to this ratio (tails)
    ratioMargin = 0.05
    if (SNratio - ratioMargin) > 0:
        SNratio -= ratioMargin

    Wms = np.percentile(timeArr,75)-np.percentile(timeArr,25)   # Using the quantile method to calculate the width
    Wms = Wms*1000                                              # Width in ms
    
    x = np.linspace(-1000,1000,10000)   # Generates x-values for the cordes function
    zeta = (6.91*10**-3)*bandWidth*(freq**-3)*(Wms**-1)*x       # Zeta function in the cordes function
    
    first = 0       # Variable indicating whether bottom or top of DM range has been found
    bot_dm = 0      # Value of the lower end of the DM range
    top_dm = 0      # Value of the upper end of the DM range
    for i in range(len(x)): # Loops through the x-values and calculates the cordes value in each step
        y = (math.pi**(1/2))*0.5*(zeta[i]**-1)*math.erf(zeta[i])
        cordes.append(y)
        if (y >= SNratio) and (first == 0): # First time the theoretical ratio goes above the actual ratio go in here
            bot_dm = x[i]                   # This x-value corresponds to the bottom DM value
            first = 1                       # Changes variable value to not go into this statement again
        if (y <= SNratio) and (first == 1): # First time the theoretical ratio goes below the actual ratio after bottom is found
            top_dm = x[i]                   # This x-value corresponds to the top DM value
            break                           # Values have been found, break out of loop
    dm_range = top_dm - bot_dm              # Theoretical allowed DM range for the current candidate
    return dm_range

clf = pickle.load(open("model.sav",'rb'))   # Loads the saved Random Forest model

counter = 0     # Counter that counts the candidate number reaching the feature loop

pos_array_mp3 = [
        8, 20, 22, 24, 1, 3, 7, 30, 31, 74, 71, 101, 102,
        103, 104, 105, 107, 112, 114, 120, 121, 122, 124,
        125, 127, 128, 131, 137, 142, 145, 146, 147, 149,
        150, 153, 133, 151
        ]

pos_array = pos_array_mp3

source_paths = []   # Array of file paths to be reviewed

# Loops through all .dat files to store them in the 'source_paths' array
for file in glob.glob(os.getcwd() + '\idir\\' + "*.dat"):
    source_paths.append(file)

try:    # Only creates folders if they don't already exist
    os.mkdir(os.getcwd() + '\idir\\' + "\\candidates")
    os.mkdir(os.getcwd() + '\idir\\' + "\\empty")
except:
    pass

shape_vals = []     # Array containing the shape feature values of the candidates
sharp_vals = []     # Array containing the sharpness feature values of the candidates
skew_vals = []      # Array containing the skewness feature values of the candidates
kurt_vals = []      # Array containing the kurtosis feature values of the candidates
kstest_vals = []    # Array containing the ks-test feature values of the candidates
class_vals = []     # Array containing the classification labels of the candidates
CCount = 0          # Counter counting the number of candidates accepted


timesPre = []
timesScan = []
timesOrder = []
timesNoise = []
timesFrac = []
timesDmlim = []
timesBurst = []
clusterPre = []
timesRange = []
timesShape = []
timesFreq = []
timesRF = []
timesClusterTot = []
timesKS = []
timesTot = []

ksDatProb = []
ksTheProb = []
KSstat = []
cordesTimes = []

timesPerMB = []
firstOrd = []
secondOrd = []

# Loops through the whole file space defined by 'source_paths'
for i in range(0,72): 
    fileSize = os.path.getsize(source_paths[i])/1024000
    start2 = timer()
    print(i)
    start = timer()
    path_index = i      # Current path index in the source_paths array
    
    file = source_paths[path_index]     # Setting which file to open
    df = DF(file) # Creates dataframe from the .dat file
    orig_X = np.array(df)   # Sets dataframe as a Numpy array
    
    X_db = np.array(df.drop(columns=['Width', 'S/N']))  # Drops width and S/N data for DBScan to run on the DM - Time space
    X = np.array(df)
    
    # Sorts the data points by DM
    points_db = X_db[X_db[:,0].argsort()]
    points = X[X[:,0].argsort()]
    
    # Lower DM limit below which the DBScan is now run as it is most likely not extragalactic
    # Speeds up the DBScan runtime
    dm_lim = 0.03*max(points_db[:,0])
    points_new = []     # Array containing the points above the cutoff point
    
    for i in range(len(points_db)):         # Loops through all points in the current file
        if (points_db[i][0] > dm_lim):      # Puts the points above the cutoff limit in the new 'points_new' array
            points_new.append(points_db[i])
    points_new = np.array(points_new)
    
    X_scaled = preprocessing.MinMaxScaler().fit_transform(points_new) # Rescales the data so that the x- and y-axes get ratio 1:1
    
    xeps = 0.025     # Radius of circle to look around for additional core points
    xmin = 3         # Number of points within xeps for the point to count as core point
    
    end = timer()
    timesPre.append(end - start)
    start = timer()
    
    clusters = DBSCAN(eps=xeps, min_samples = xmin).fit_predict(X_scaled)   # Clustering algorithm, returns array with cluster labels for each point
    
    end = timer()
    timesScan.append(end - start)
    
    # Re-inserts bottom points with labels -1 for RFI
    length = len(points) - len(clusters)
    clusters = np.insert(clusters,0,np.full(length,-1))
    
    startOrd = timer()
    startF = timer()
    labels_arr = clusterSort(clusters, points)  # Creates an array of arrays of points where each index contains the points of one cluster
    endF = timer()
    firstOrd.append(endF - startF)
    startS = timer()
    clusterOrder(clusters)                      # Re-orders the data so that labels being at -1 and counts upwards in increments of 1
    endS = timer()
    secondOrd.append(endS - startS)
    endOrd = timer()
    timesOrder.append(endOrd - startOrd)
    
    start = timer()
    # Noise condition for Nevents<Nmin => noise
    N_min = 20 # Tuneable number (set to match Karako)
    for q in range(1,len(np.unique(clusters))):
        if (len(labels_arr[q]) < N_min):        # Gives points of clusters with less than N_min events the RFI lable of -1
            clusters = np.where(clusters == (q - 1), -1, clusters)              
    end = timer()
    timesNoise.append(end - start)
    
    #Re-order        
    labels_arr = clusterSort(clusters, points)
    clusterOrder(clusters)
    #break
    dm_lim = 40         # Increased DM-limit to check for clusters with 'fraction' of their points below this limit
    fraction = 0.05     # Size of the fraction of points allowed in below 'dm_lim'
    
    start = timer()
    # Condition that sets all clusters with 'fraction' of its points below dm_lim to also be classified as RFI
    for q in range(1,len(np.unique(clusters))):
        num_temp = int(round(fraction*len(labels_arr[q]),0))    # Calculates how many points of a label a certain fraction corresponds to and rounds to nearest integer
        temp = sort(labels_arr[q][:,0],num_temp)                # Returns the 'num_temp' lowest dms in labels_arr[q]
    
        if ((len(temp) > 0) and (max(temp) < dm_lim)):          # If the highest number in temp is below dm_lim then so is the rest in 'temp'
            clusters = np.where(clusters == (q - 1), -1, clusters) 
            
    end = timer()
    timesFrac.append(end - start)
      
    start = timer()              
    # Condition that sets all points with dm below dm_lim to be classified as RFI    
    for i in range(len(clusters)):
        if (points[i][0] <= dm_lim):
            clusters[i] = -1
    end = timer()
    timesDmlim.append(end - start)
    
    # Re-order
    labels_arr = clusterSort(clusters, points)
    clusterOrder(clusters)
    
    start = timer()
    # Burst duration condition
    for q in range(1,len(np.unique(clusters))):
        upper = np.quantile(labels_arr[q][:,1], 0.8)
        lower = np.quantile(labels_arr[q][:,1], 0.2)

        if (upper - lower) >= 1:            # If the time between the quantiles is longer than 1s set cluster to RFI
            clusters = np.where(clusters == (q - 1), -1, clusters)
    end = timer()
    timesBurst.append(end - start)
     
    # Re-order
    labels_arr = clusterSort(clusters, points)
    clusterOrder(clusters)
    
    least_acc = []      # Contains the points of all candidates classified as 'least acceptable'
    good = []           # Contains the points of all candidates classified as 'good'
    excellent = []      # Contains the points of all candidates classified as 'excellent'
    labelNumber = 0     # Label number of clusters in same files, used to separate multiple clusters of equal classification

    # Loops through all remaining clusters to exclude further clusters, calculate feature values, and classify them using Random Forest
    for q in range(1,len(np.unique(clusters))):
        start1 = timer()
        
        start = timer()
        signalToDm = list(zip(labels_arr[q][:,0], labels_arr[q][:,2]))  # Array containing DM - SN data
        signalToDm = np.array(signalToDm)
        min_val = min(signalToDm[:,1])                                  # Finds the lowest SN-value in the candidate array

        scaled_signal = preprocessing.MinMaxScaler().fit_transform(signalToDm)  # Preprocesses signalToDM to used for sharpness condition

        # Sets y = 0 for visualisation
        for i in range(len(signalToDm[:,1])):
            signalToDm[:,1][i] = signalToDm[:,1][i] - min_val
            
        # Splitting into chunks of equal number of events in each
        split_param = 7 # Number of chunks to be split into
        dummy = np.array_split(signalToDm,split_param)
        dummy2 = np.array_split(scaled_signal,split_param) # For the sharpness condition
        
        meanSN = []     # Contains mean SN value of each chunk 
        meanDM = []     # Contains mean DM value of each chunk
        s_meanSN = []   # Same for sharpness calculation
        s_meanDM = []
        
        # Loops through the chunks, calculates the relevant mean values and puts them into the appropriate arrays
        for i in range(len(dummy)):
            tempSN = np.mean(dummy[i][:,1])
            tempDM = np.mean(dummy[i][:,0])
            meanSN.append(tempSN)
            meanDM.append(tempDM)
            
            # For sharpness calculation
            s_tempSN = np.mean(dummy2[i][:,1])
            s_tempDM = np.mean(dummy2[i][:,0])
            s_meanSN.append(s_tempSN)
            s_meanDM.append(s_tempDM)
        
        max_val = max(meanSN + min_val) # Array containing the real (not reduced) peak SN values in each chunk
        
        max_ind = np.argmax(meanSN)     # Finds the index for the highest SN bin value
        peakMeanDm = meanDM[max_ind]    # The corresponding DM for this bin value
        
        end = timer()
        clusterPre.append(end - start)
        start = timer()
        
        upper_dm_range = cordes(labels_arr[q][:,1], meanSN[max_ind])        # The theoretically widest allowed DM range of candidate
        dm_diff = max(labels_arr[q][:,0]) - min(labels_arr[q][:,0])         # The DM range spanned by the actual data
        end = timer()
        timesRange.append(end - start)
        # Condition that excludes the candidates with SN peaks too far from the centre, as well as candidates spanning a range that is too wide
        # Using 2 times the upper_dm_range to use a large safety margin
        if (max_ind > 4) or (max_ind < 2) or (dm_diff > 2*upper_dm_range):    
            for i in range(len(clusters)):
                if (clusters[i] == q - 1):
                    clusters[i] = -1
            end2 = timer()
            timesTot.append(end2 - start2)
        
        # All candidates that haven't been excluded thus far go in here
        else:
            counter += 1        # Final candidate number reaching this point
            freq_arr = []       # Probability frequency distribution representation of the DM - SN data
            
            start = timer()
            weight_1 = -1       # Score weight if ratio is less than 1 but more than 1 - check_1
            weight_2 = -0.3     # Score weight if ratio is less than 1 - check_1, but more than 1 - check_2
            weight_3 = 1        # Score weight if ratio is less than 1 - check_2
            weight_4 = -1       # Score weight if ratio is more than 1
            check_1 = 0.075
            check_2 = 0.15
            score = [0,1.3,2.5,2.5]                         # Scoring system where one index step corresponds to one step from peak bin
            max_score = 2*(score[0] + score[1] + score[2])  # Maximum possible score
            
            rating = 0  # Rating score after weight and scores have been applied
            
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
            end = timer()
            timesShape.append(end - start)
            
            start = timer()
            # Converts the S/N-DM plot into a probability frequency plot
            # Instead of each point in DM space having a corresponding S/N y-value
            # there will be an array containing a number of DM elements proportional to its S/N value
            normal_snRatios = (signalToDm[:,1] + 9)/(max(signalToDm[:,1]) + 9)
            for i in range(len(signalToDm)):
                temp_arr = []
                frequency = int((normal_snRatios[i])*1000)      # Needs to be an integer, timed it by 1000 to reduce rounding errors, proportions still same
                temp_arr = [signalToDm[i][0]] * frequency   # Creates the corresponding number of elements and adds it to the array
                freq_arr.extend(temp_arr)
            end = timer()
            timesFreq.append(end - start)
            
            diff_SN = max(s_meanSN) - (0.5*s_meanSN[0] + 0.5*s_meanSN[-1])
            diff_DM = s_meanDM[-1] - s_meanDM[0] #?????center this around peak
            
            # FEATURES
            sharp_ratio = diff_SN/(diff_SN + diff_DM)           # Height/width   
            shape_conf = rating/max_score                       # Shafe conf feature
            skewness = skew(freq_arr, axis = 0)                 # Skewness feature
            kurt = kurtosis(freq_arr, axis = 0, fisher = True)  # Kurtosis feature
            
            start = timer ()
            ks_stat = ks_cordes(signalToDm[:,0],signalToDm[:,1],labels_arr[q][:,1],meanDM[max_ind])     # KS feature
            end = timer()
            timesKS.append(end - start)
            # Adds the feature values to the corresponding arrays
            shape_vals.append(shape_conf)
            sharp_vals.append(sharp_ratio)
            skew_vals.append(skewness)
            kurt_vals.append(kurt)
            kstest_vals.append(ks_stat)
            
            features = [shape_conf, sharp_ratio, skewness, kurt, ks_stat]       # Arrays of features to be ran through Random Forest model
            start = timer()
            results = clf.predict_proba([features])                             # Runs Random Forest model on features arrays
            end = timer()
            timesRF.append(end - start)
            
            timeDiff = upper - lower
            
            end1 = timer()
            timesClusterTot.append(end1 - start1)
            end2 = timer()
            timesTot.append(end2 - start2)
            timesPerMB.append((end2 - start2)/fileSize)
            if (results[0][1] > 0.8):       # If the final rating from Random Forest is more than 0.8 goes in here
                for m in labels_arr[q]:     # Adds all points in this cluster to 'excellent' array
                    excellent.append(np.append(m,labelNumber))
                labelNumber += 1
                CCount += 1
                """
                fig = plt.figure()
                ax = fig.add_subplot(111)
                ax.scatter(signalToDm[:,0], signalToDm[:,1], s = 6)
                ax.set_title(str(counter))
    
                fig1 = plt.figure()
                ax1 = fig1.add_subplot(111)
                ax1.scatter(points[:,1], points[:,0], s = 6, color = '0.7', vmin = -1)
                ax1.scatter(labels_arr[q][:,1], labels_arr[q][:,0], s = 6, color = 'r', vmin = -1)
                ax1.set_title(str(i))
                ax1.set_xlabel(str(timeDiff))"""
                
            elif (results[0][1] > 0.65):    # If the final rating from Random Forest is more than 0.65 but less than 0.8, goes in here
                for m in labels_arr[q]:     # Adds all points in this cluster to 'good' array
                    good.append(np.append(m,labelNumber))
                labelNumber += 1
                CCount += 1
                """
                fig = plt.figure()
                ax = fig.add_subplot(111)
                ax.scatter(signalToDm[:,0], signalToDm[:,1], s = 6)
                ax.set_title(str(counter))
    
                fig1 = plt.figure()
                ax1 = fig1.add_subplot(111)
                ax1.scatter(points[:,1], points[:,0], s = 6, color = '0.7', vmin = -1)
                ax1.scatter(labels_arr[q][:,1], labels_arr[q][:,0], s = 6, color = 'r', vmin = -1)
                ax1.set_title(str(i))
                ax1.set_xlabel(str(timeDiff))"""
                
            elif (results[0][1] > 0.5):     # If the final rating from Random forest is more than 0.5 but less than 0.65, goes in here
                for m in labels_arr[q]:     # Adds all points in this cluster to 'least acceptable' array
                    least_acc.append(np.append(m,labelNumber))
                labelNumber += 1
                CCount += 1
                """
                fig = plt.figure()
                ax = fig.add_subplot(111)
                ax.scatter(signalToDm[:,0], signalToDm[:,1], s = 6)
                ax.set_title(str(counter))
    
                fig1 = plt.figure()
                ax1 = fig1.add_subplot(111)
                ax1.scatter(points[:,1], points[:,0], s = 6, color = '0.7', vmin = -1)
                ax1.scatter(labels_arr[q][:,1], labels_arr[q][:,0], s = 6, color = 'r', vmin = -1)
                ax1.set_title(str(i))
                ax1.set_xlabel(str(timeDiff))
            
            plt.show()"""
            
            # Adds the correct class label for the cluster to the class_vals array.
            # 1 = candidate, 0 = no candidate
            if (counter in pos_array):
                class_vals.append(1)
            else:
                class_vals.append(0)
            
    #Re-order        
    labels_arr = clusterSort(clusters, points)
    clusterOrder(clusters)
    
print(CCount)
print(counter)
print("Preprocessing: " + str(np.round(np.mean(timesPre), 4)) + "s")
print("DBScan: " + str(np.round(np.mean(timesScan), 4)) + "s")
print("Re-ordering: " + str(np.round(np.mean(timesOrder), 4)) + "s")
print("First ordering: " + str(np.round(np.mean(firstOrd), 4)) + "s")
print("Second ordering: " + str(np.round(np.mean(secondOrd), 4)) + "s")
print("Noise condition: " + str(np.round(np.mean(timesNoise), 4)) + "s")
print("Fraction condition: " + str(np.round(np.mean(timesFrac), 4)) + "s")
print("DM limit condition: " + str(np.round(np.mean(timesDmlim), 4)) + "s")
print("Burst duration condition: " + str(np.round(np.mean(timesBurst), 4)) + "s")
print("Cluster preprocessing: " + str(np.round(np.mean(clusterPre), 4)) + "s")
print("DM range condition: " + str(np.round(np.mean(timesRange), 4)) + "s")
print("Shape calculation: " + str(np.round(np.mean(timesShape), 4)) + "s")
print("Prob. Freq. calc.: " + str(np.round(np.mean(timesFreq), 4)) + "s")
print("KS-test: " + str(np.round(np.mean(timesKS), 4)) + "s")
print("Random Forest: " + str(np.round(np.mean(timesRF), 4)) + "s")
print("Tot cluster time: " + str(np.round(np.mean(timesClusterTot), 4)) + "s")
print("Total per file: " + str(np.round(np.mean(timesTot), 4)) + "s")
print("Time per MB: " + str(np.round(np.mean(timesPerMB), 4)) + "s")
print("\n")
print("Cordes y-vals: " + str(np.round(np.mean(cordesTimes), 4)) + "s")
print("KS data generation: " + str(np.round(np.mean(ksDatProb), 4)) + "s")
print("KS theo generation: " + str(np.round(np.mean(ksTheProb), 4)) + "s")
print("KS stat calc.: " + str(np.round(np.mean(KSstat), 4)) + "s")


"""
    least_acc = np.array(least_acc)
    good = np.array(good)
    excellent = np.array(excellent)
    
    total = []
    if_var = 0
    
    # Adds a column to the numpy array with an integer corresponding to the cluster classification
    # 3 = excellent, 2 = good, 1 = least acceptable
    if (len(excellent) > 0):
        excellent_c = np.full((len(excellent),6), 3, dtype = float)
        excellent_c[:,:-1] = excellent
        total.extend(excellent_c)
        if_var += 1
        CCount += 1
    if (len(good) > 0):
        good_c = np.full((len(good),6), 2, dtype = float)
        good_c[:,:-1] = good
        total.extend(good_c)
        if_var += 1
        CCount += 1
    if (len(least_acc) > 0):
        least_acc_c = np.full((len(least_acc),6), 1, dtype = float)
        least_acc_c[:,:-1] = least_acc
        total.extend(least_acc_c)
        if_var += 1
        CCount += 1
        
    total = np.array(total)         # Sets the total array containing feature values, class labels, and cluster numbers to Numpy array
    print(excellent)
    print(good)
    print(least_acc)

    if len(total) > 0:              # If the 'total' array contains candidates then go in here
        dataframe = pd.DataFrame({'DM': total[:,0],         # Creates a dataframe with feature values, class labels, and cluster numbers
                                  'Time': total[:,1],
                                  'S/N': total[:,2],
                                  'Width': total[:,3],
                                  'Class': total[:,5],
                                  'Cluster Number': total[:,4]})
        
        new_name = source_paths[path].split("idir\\")[1].replace('.dat','_c.csv')               # Name of the class file to be created
        dataframe.to_csv(os.getcwd() + '\\idir\\' + "\\candidates\\" + new_name, index = False) # Saves csv file of the above dataframe
        new_path = os.getcwd() + '\\idir\\' + "\\candidates\\" + source_paths[path].split("idir\\")[1]  # Path to the created file
        
        if (os.path.isfile(new_path)) == False:     # Moves the .dat file to the 'candidates' directory only if there isn't already an
            os.rename(file, new_path)               # identical .dat file there.
            
    else:                           # If there are no candidates then move the .dat file to the 'empty' directory
        new_path = os.getcwd() + '\\idir\\' + "\\empty\\" + source_paths[path].split("idir\\")[1]
        if (os.path.isfile(new_path)) == False:
            os.rename(file, new_path)"""

         
    