import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import math

sns.set()

myX = [10,3,1,15,14,0,16,5,11,2,12,9,13,7,8,6,4]


left_Delay = [42.85,14.61,13.0,51.72,52.6,13.0,34.58,17.38,46.66,13.72,46.06,39.93,65.43,35.7,42.95,19.52,15.58]
right_Delay = [69.15,15.39,13.0,100.28,93.4,13.0,75.42,18.62,73.34,14.28,77.94,70.07,114.57,58.3,71.05,20.48,16.42]

left_Droprate = [22.59,0.02,0.0,61.57,55.16,0.0,72.93,0.45,27.85,0.0,35.38,20.88,46.62,11.22,14.95,0.89,0.19]
right_Droprate = [29.61,0.78,0.0,71.43,63.64,0.0,80.47,3.15,34.35,0.0,42.62,26.52,55.18,16.58,21.45,2.71,0.83]

left_Overhead = [0.430200,0.181360,0.177580,0.388410,0.368630,0.177470,0.334200,0.202580,0.443910,0.178450,0.430510,0.405490,0.382510,0.404790,0.402000,0.226550,0.190030]
right_Overhead = [0.495870,0.183960,0.177820,0.526660,0.473170,0.177580,0.459600,0.212030,0.510030,0.179290,0.505710,0.450750,0.474900,0.440170,0.447300,0.238310,0.193370]

def plotHistogram(figNumHis,dataSet,binAmount,fileNameHist):
#End to end delay plots
    plt.figure(figNumHis)
    histogram_delay = plt.hist(dataSet,bins=binAmount)

    my_unit = 'ms'

    histogram_delay = plt.xlabel('End-to-end delay (${}$)'.format(my_unit))

    histogram_delay = plt.ylabel('Amount in bins')

    plt.savefig('/home/jonas/Documents/p6/{}.png'.format(fileNameHist))
    print("Following Histogram saved: {}.png \n".format(fileNameHist))
    #plt.show()

def plotConfidence(figNumConf,x_interval,left_side,right_side,middle,fileNameConf):
    
    sortedLeft = [x for _,x in sorted(zip(x_interval,left_side))]
    sortedRight = [x for _,x in sorted(zip(x_interval,right_side))]
    print(x_interval)
    #x_interval.sort()
    print(x_interval)
    plt.figure(figNumConf)
    plt.plot(sorted(x_interval),sortedLeft,linestyle='dashed')
    #plt.plot(sorted(x_interval),middle)
    plt.plot(sorted(x_interval),sortedRight,linestyle='dashed')
    plt.savefig('/home/jonas/Documents/p6/{}.png'.format(fileNameConf))

    print("Following Confidenceplot saved: {}.png \n".format(fileNameConf))
    plt.show()   

plotConfidence(1,myX,left_Droprate,right_Droprate,1,'confPlotDroprate')

plotConfidence(2,myX,left_Delay,right_Delay,1,'confPlotDelay')

plotConfidence(3,myX,left_Overhead,right_Overhead,1,'confPlotOverhead')