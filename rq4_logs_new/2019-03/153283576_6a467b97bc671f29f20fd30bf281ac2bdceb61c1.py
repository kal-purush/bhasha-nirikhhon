import numpy as np
import matplotlib as mpl
import glob, os
import warnings
from matplotlib import pyplot as plt
import math
import random
import numpy as np

warnings.filterwarnings("ignore", category=mpl.cbook.mplDeprecation)
warnings.filterwarnings("ignore", category=RuntimeWarning)

freq = 0.334
bandWidth = 64

dmRangeBot = 100
dmRangeTop = 800
snRangeBot = 10
snRangeTop = 40
upperTime = 50      # time in s
upperDur = 600      # time in ms
quantFrac = 0.8
numBursts = 10
bursts = []

for k in range(numBursts):
    
    cordes = []             # y-values of the theoretical cordes function
    yCordesArr = []
    xCordesArr = []
    tArr = []
    Wms = np.random.uniform(0,upperDur)              # Must be in milliseconds
    timeRange = Wms/(2*quantFrac)
    dmMid = np.random.uniform(dmRangeBot, dmRangeTop)
    peakSN = np.random.uniform(snRangeBot, snRangeTop)
    xRange = 50
    stDevSN = peakSN*0.15
    stDevDM = xRange*0.3
    numPoints = 100
    start = -xRange
    step = 2*xRange/numPoints
    fraction = 0.40
    count = 0
    
    for i in range(numPoints + 1):
        tempVar1 = random.uniform(0,1)
        if tempVar1 >= fraction:
            continue
        else:
            count += 1
            xTemp = start + step*i
            if xTemp == 0:
                xTemp = 0.00000001
        
            devSN = np.random.normal(0,stDevSN,1)
            devDM = np.random.normal(0,stDevDM,1)
            
            xCordesArr.append(dmMid + xTemp + devDM)

            zeta = (6.91*10**-3)*bandWidth*(freq**-3)*(Wms**-1)*(xTemp + devDM)
            yCordesArr.append(math.pi**(1/2)*0.5*(zeta**-1)*math.erf(zeta)*(peakSN + devSN))
            
            timeVar = np.random.uniform(-timeRange,timeRange)/1000
            tArr.append(timeVar)
    
    totArr = np.zeros([count, 3])

    totArr[:,0] = np.array(np.transpose(xCordesArr))
    totArr[:,1] = np.array(np.transpose(tArr))
    totArr[:,2] = np.array(np.transpose(yCordesArr))
    print(totArr)
    bursts.append(totArr)
    
#print(np.array(bursts))
    
"""
x = np.linspace(-200,200,2000)                              # X-values for cordes function
zeta = (6.91*10**-3)*bandWidth*(freq**-3)*(Wms**-1)*x       # Zeta function, see Cordes & M


for i in range(len(x)):
    cordes.append((math.pi**(1/2))*0.5*(zeta[i]**-1)*math.erf(zeta[i]))
cordes = np.array(cordes)
cordes = peakSN*cordes

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(x, cordes)
ax.scatter(xCordesArr,yCordesArr, s = 40)
ax.set_xlabel("DM")
ax.set_ylabel("S/N")
ax.set_xlim(-xRange - 5, xRange + 5)
ax.set_title("Cordes function")
plt.show()"""