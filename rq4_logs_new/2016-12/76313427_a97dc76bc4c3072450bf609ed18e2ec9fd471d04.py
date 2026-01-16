#!/usr/bin/python  
# -*- coding: utf-8 -*-  
from chaco.plot import Plot


'''
Created on 2016年12月13日

@author: Administrator
'''

import pandas as pd
from pandas import Series
import numpy as np
import random
import matplotlib.pyplot as plt
from pandas_datareader import data as web

'''
a=np.random.standard_normal((9,4))
a.round(6)
# print(a)

df=pd.DataFrame(a)
df.columns=[['No1','No2','No3','No4']]
dates=pd.date_range('2015-1-29',periods=9,freq='M')
#print(dates)

df.index=dates

#print(df)
#print(df.mean)
#print(np.sqrt(df))

print(df['No1'])

df['Quarter']=['Q1','Q1','Q1','Q2','Q2','Q2','Q3','Q3','Q3']
print(df)

groups=df.groupby('Quarter')

print(groups.mean())
print(groups.max())
print(groups.size())

df['Odd_Even']=['Odd','Even','Odd','Even','Odd','Even','Odd','Even','Odd']
groups=df.groupby(['Quarter','Odd_Even'])
print(groups.size())
'''
DAX=web.DataReader(name='GOOG', data_source='yahoo', start='2015-1-1', end='2015-12-31')
DAX['42d']=Series.rolling(DAX['Close'],window=42).mean()

DAX[['Close','42d']].plot(figsize=(8,5))
plt.show()
'''
DAX['Ret_Loop']=0.0
for i in range(1,len(DAX)):
    DAX['Ret_Loop'][i]=np.log(DAX['Close'][i]/DAX['Close'][i-1])
    
print(DAX[['Close','Ret_Loop']])
'''
DAX['Return']=np.log(DAX['Close']/DAX['Close'].shift(1))
print(DAX[['Close','Return']])
