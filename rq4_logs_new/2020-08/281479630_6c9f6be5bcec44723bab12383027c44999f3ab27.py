# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from uncertainties import ufloat
from uncertainties import umath
from openpyxl import load_workbook
from scipy.optimize import curve_fit

data = pd.read_excel('HO2+OH=H2O+O2.xlsx','HO2+OH=H2O+O2_Ref')

data.drop('comments',axis=1, inplace=True)
data.dropna(axis=0,how='any',inplace=True)

minT = max(min(data['min T']),300)
maxT = min(max(data['max T']),3500)

def rate_function(x, a,b,c,a1,b1,c1):
    return a * (x/298) ** b * np.exp(-c*1000/8.314/x) + a1 * (x/298) ** b1 * np.exp(-c1*1000/8.314/x)
#    return a * (x/298) ** b * np.exp(-c*1000/8.314/x)
#print(minT, maxT)

Tlist = np.arange(minT, maxT + 10, 10)
Authorlist = np.array(data['Author'])
klist = np.zeros((len(Tlist),len(Authorlist)))
kulist = np.zeros((len(Tlist),len(Authorlist)))
#print(klist)
for i in range(len(data.index)):
#    print(i)
    A = ufloat(data.iloc[i]['A'],data.iloc[i]['Au'])
    n = ufloat(data.iloc[i]['n'],data.iloc[i]['nu'])
    E = ufloat(data.iloc[i]['E'],data.iloc[i]['Eu'])
    
    for j in range(len(Tlist)):
        T = j * 10 + minT
#        print(T)
        if data.iloc[i]['min T'] <= T <= data.iloc[i]['max T']:
            k = (A * (T/298) ** n) * umath.exp(-E*1000/8.314/T)
            klist[j][i] = k.n
            kulist[j][i] = k.s

        else:
            klist[j][i] = np.nan
            kulist[j][i] = np.nan

rate = pd.DataFrame(klist, columns=Authorlist, index=Tlist)
rateu = pd.DataFrame(kulist, columns=Authorlist, index=Tlist)
rate.dropna(axis=0,how='all',inplace=True)
print(len(rate.index))
klist=rate.values.tolist()

rateu.dropna(axis=0,how='all',inplace=True)
Tlist=rateu.index.tolist()
kulist=rateu.values.tolist()

print(Tlist)


weights = 1/np.square(kulist)            
average_rate = (np.nansum(np.multiply(klist,weights), axis = 1)) / np.nansum(weights, axis = 1)
#print(average_rate)
uncertainty = 1/np.sqrt(np.nansum(weights, axis = 1))  
#print(uncertainty)
 
    
simu_average_rate=[x for x in average_rate if ~np.isnan(x)]
simu_uncertainty=[x for x in uncertainty if np.isfinite(x)]
#print(simu_average_rate,simu_uncertainty)

p0 = 1.5E-10, -0.1, 66.00, 

pfit,pcov = curve_fit(rate_function, Tlist, average_rate,
                    sigma=uncertainty, maxfev=10000)
perr = np.sqrt(np.diag(pcov))
print(pfit,perr)

evaluated_rate = [rate_function(T,*pfit) for T in Tlist]   
reverseTlist=[10000/T for T in Tlist]    
plt.plot(reverseTlist,average_rate,'bo')  
plt.plot(reverseTlist,evaluated_rate,'r--') 

evaluated = pd.DataFrame({'average_rate':average_rate,
                        'uncertainty':uncertainty,
                        'evaluated_rate':evaluated_rate,
                        '10000/T':reverseTlist,
                        'Temperature':Tlist})
#average = pd.DataFrame(average_rate,columns=Authorlist,index=Tlist)
#print(rate)
path="HO2+OH=H2O+O2.xlsx"     
book = load_workbook(path)
writer = pd.ExcelWriter(path, engine = 'openpyxl')
writer.book = book
rate.to_excel(writer,"rate")
rateu.to_excel(writer,"rateu")
evaluated.to_excel(writer,"evaluated",index=None)
writer.save()
writer.close()