#!/usr/bin/python3
"""
    Program:        patientcurve.py
    Author:         Peter Southwick
    Date:           07/11/16
    Description:    A Short program that .

"""
import numpy as np
import matplotlib.pyplot as plot
import tabulate as tab


def factors(marray):  # Compute factors in marray
    n = marray.size
    mf = np.zeros(n-1)  # array of factors
    for j in range(n-1):
        mf[j] = marray[j+1]/marray[j]
    return mf

providedpatientdata = np.array([5500, 8500, 13500, 20500, 29500, 40500, 53500, 68500])

tempFactors = factors(providedpatientdata)  # Calculate the factors from the given temp data
firstDiff = np.diff(providedpatientdata)  # Calculate the first difference
secondDiff = np.diff(firstDiff)  # Calculate the second difference
tempMean = np.mean(tempFactors)  # Calculate the mean of the factors

calculatedTemps = np.zeros(providedpatientdata.size)
calculatedTemps[0] = providedpatientdata[0]
for i in range(providedpatientdata.size - 1):  # Calculate our estimated temp totals using the factor mean
    calculatedTemps[i+1] = tempMean * calculatedTemps[i]

xi = 0.0
xf = 8.0
sn = np.size(providedpatientdata)
x = np.linspace(xi, xf, sn)
deg = 3  # Degree of our desired polynomial
poly = np.polyfit(x, providedpatientdata, deg)
xc = np.linspace(xi, xf, 30)  # new x points
yc = np.polyval(poly, xc)  # new y points


f, linearr = plot.subplots(2, sharex=True)
linearr[0].set_title('Provided data')
linearr[0].plot(x, providedpatientdata, marker='o', linestyle='-')
linearr[1].set_title('Calculated data')
linearr[1].plot(xc, yc, marker='o', linestyle='-')
plot.show()



print("Original list:", providedpatientdata)
print("First diff:", firstDiff)
print("Second diff:", secondDiff)
print("Factors:", tempFactors)
print("Mean:", tempMean)
print("Calculated Temps using the mean of factors: ", calculatedTemps)
print("Our PolyFit Coefficients:", poly)
print("Evaluation of our curve fitted polynomial")
for j in range(30):
    print(xc[j], yc[j])