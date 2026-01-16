import numpy as np
import matplotlib as mpl
import glob, os
import warnings
from matplotlib import pyplot as plt
import math
import random
import pandas as pd
import sys


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

warnings.filterwarnings("ignore", category=mpl.cbook.mplDeprecation)
warnings.filterwarnings("ignore", category=RuntimeWarning)

intention = input("Simulate plots (s), or generate data to use (g)? ")

while intention != "s" and intention != "g":
    print("Invalid input, please indicated by entering either ""s"" or ""g"".")
    intention = input("Simulate plots (s), or generate data to use (g)? ")

numBursts = input("Enter number of candidates to simulate: ")

while True:
    try:
        numBursts = int(numBursts)
        break
    except:
        print("Invalid input, please indicate choice using an integer.")
        numBursts = input("Enter number of candidates to simulate: ")

if intention == "g":
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
upperDur = 600                  # Upper possible duration of burst in milliseconds
quantFrac = 0.8                 # Fraction over which the quantile method to calculate duration is used
fraction = 0.75                 # Probability of there being a point in a single DM step
noiseFraction = 0.1             # Fraction of noise events in a candidate

bursts = []                     # All bursts are stored here, array of arrays
counter = 0


source_paths = []   # Array of file paths to be reviewed

# Loops through all .dat files to store them in the 'source_paths' array
for file in glob.glob(os.getcwd() + '\idir\\' + "*.dat"):
    source_paths.append(file)


while counter < numBursts:
    progressBar(counter, numBursts)
    path_index = int(round(np.random.uniform(0,len(source_paths) - 1)))
    file = source_paths[path_index]      # Setting which file to open
    data = np.array(DF(file))   # Creates dataframe from the .dat file
    
    lab = np.zeros((len(data[:,0]),1))
    data = np.column_stack((data, lab))

    if max(data[:,0]) < 170:
        continue
    else:
        counter += 1
        
        upperTime = max(data[:,1]) - upperDur/2000
        
        snArr = []              # All SN values of a burst
        dmArr = []              # All DM value of a burst
        tArr = []               # All times for the burst
        wArr = []
        labArr = []
        timeMid = np.random.uniform(1,upperTime)                # Average time of detection for a burst
        dmMid = np.random.uniform(dmRangeBot, dmRangeTop)       # Average DM of the burst
        peakSN = np.random.uniform(snRangeBot, snRangeTop)      # Peak SN of the burst
        
        dmWidthTop = 0.2*dmMid
        upperTime = max(data[:,1]) - upperDur/2000
        
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
        
        finalArr = np.vstack((data,totArr))
        
        if intention == "s":
            fig = plt.figure()
            ax = fig.add_subplot(111)
            ax.scatter(finalArr[:,1],finalArr[:,0], s = 5, c = finalArr[:,4], cmap = "brg", vmin = -1, alpha = 0.6)
            ax.set_xlabel("Time")
            ax.set_ylabel("DM")
            ax.set_title("Cordes function")
            plt.show()
        else:
            
            finalArr.reshape(-1)
            fileEnding = "(" + str(counter) + ").dat"
            fileName = file.split("idir\\")[1].replace('.dat',fileEnding)               # Name of the class file to be created
            
            finalArr.astype(np.float32).tofile(os.getcwd() + '\\odir\\' + folderName + "\\" + fileName)
            
            #dataframe.to_csv(os.getcwd() + '\\odir\\' + folderName + "\\" + fileName, index = False)
            
        


