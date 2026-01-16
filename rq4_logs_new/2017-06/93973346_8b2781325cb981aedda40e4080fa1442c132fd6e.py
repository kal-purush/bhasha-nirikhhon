
# coding: utf-8

# Relative Strength Index (RSI) 
# 
# Developed by J. Welles Wilder, the Relative Strength Index (RSI) is a momentum oscillator that measures the speed and change of price movements. RSI oscillates between zero and 100. Traditionally, and according to Wilder, RSI is considered overbought when above 70 and oversold when below 30. 
# 
# There are 2 common ways of calculating Relative Strength Index. 
#     1. Using Simple Moving Average (SMA)
#     2. Using Exponential Moving Average (EMA)
# 
# The following python script aims to calculate the RSI for First Solar stock (FSLR) using both methods. 
#     

# In[427]:

import quandl
import pandas as pd
import matplotlib.pyplot as plt 
get_ipython().magic(u'matplotlib inline')
import numpy as np


# In[428]:

# Define Start and End Dates for RSI calculation 
start_date = "2016-03-18"
end_date = "2017-03-18"


# In[429]:

# Getting Data from Quandl 
df = quandl.get('WIKI/FSLR', start_date = start_date, end_date = end_date)

# Populating the DataFrame with Only the Adjusted Close Numbers
df = df['Adj. Close']

# Subtracting today close number with yesterday close to calculate 
# daily gain(loss)
diff = df.diff()

# Removing first row (NaN), because it did not have previous close 
diff = diff[1:]

plot = df.plot()
plot.set_title('Fisrt Solar Stock Price History', fontsize = 18);
plot.set_ylabel('Price', fontsize= 12);


# In[419]:

# Defining the function to calculate moving average based on 
# the exponential moving average method, explanations are in line 

def ExponentialMovingAverage(series, lookBack):
    
    # Initial exponential moving average (ema) list
    ema = []
    # Counter to iterate through the gain/loss list is set to 1 
    # because the current EMA pointer is the position 1
    j = 1

    # There are three steps to calculating an exponential moving average. 
    # First, calculate the simple moving average. 
    # An exponential moving average (EMA) has to start somewhere so a 
    # simple moving average (sma) is used as the previous period's EMA in the first calculation.  
    sma = sum(series[:lookBack]) / lookBack
    ema.append(sma) 
    
    #  Second, calculate the weighting multiplier. 
    #  The multiplier assigns weight for the current index
    multiplier = 2 / float(1 + lookBack)

    #EMA(current) = ( (Current(gain/loss) - EMA(prev) ) x Multiplier) + EMA(prev)
    ema.append(( (series[lookBack] - sma) * multiplier) + sma)

    #Calculating the EMA for the rest of the values
    for i in series[lookBack+1:]:
        temp = ( (i - ema[j]) * multiplier) + ema[j]
        j = j + 1
        ema.append(temp)

    return ema


# In[ ]:

# Defining the function to calculate moving average based on 
# the simple moving average (SMA) method

def SimpleMovingAverage(series, lookBack):
    sma = []
    temp = 0 
    
    # Moving average are only applicable for indices bigger than 
    # lookBack period, values in list prior to that get assigned 
    # averages up to their
    for i in range (0, lookBack):
        temp = temp + series[i]
        sma.append(float(temp)/(i+1))
        
    for i in range(lookBack, len(series)):
        temp = temp - series[i-lookBack] +series[i]
        sma.append(float(temp)/lookBack)
    
    return sma 


# In[420]:

# The default time frame for comparing up periods to down periods is 14, as in 14 trading days.

# Defining look back period 
lookBack = 14
# Defining offset (the first value difference of indeces is a non-number, NaN)
offset = 1

# Defining lists which will hold gains and losses only, zeroes out losses in gain list 
# zeroes out gains in loss list, that way calculating averages of gains (only) 
# or losses (only) is made simple 

gain = diff.copy()
loss = diff.copy()
gain[gain<0] = 0 
loss[loss>0] = 0



# In[422]:

# Calculating the Exponential Weighted Moving Average (EWMA)
ewaGain = np.array(ExponentialMovingAverage(gain, lookBack))
ewaLoss = np.array(ExponentialMovingAverage(loss.abs(), lookBack))


# In[423]:

# Calculating the RS based on EWMA:
# RS = Average gain of up periods during the specified time frame 
#      / Average loss of down periods during the specified time frame
RS_ema = ewmaGain / ewmaLoss

# Calculating the RSI based on EWMA
RSI_ema = 100.0 - (100.0 / (1.0 + RS_ewma))


# In[424]:

# Calculating the Simple Moving Average 

smaGain = np.array(SimpleMovingAverage(gain, lookBack))
smaLoss = np.array(SimpleMovingAverage(loss.abs(), lookBack))

smaGain = smaGain[lookBack-offset:]
smaLoss = smaLoss[lookBack-offset:]


# In[425]:

# Calculating the RS based on SMA:
# RS = Average gain of up periods during the specified time frame 
#      / Average loss of down periods during the specified time frame

RS_sma = smaGain/smaLoss
RSI_sma = 100.0 - (100.0 / (1.0 + RS_sma))




# In[426]:

# Plotting both RSIs 

plt.plot(RSI_sma, 'b-', label='RSI : SMA');
plt.plot(RSI_ema, 'g-', label='RSI : EMA');
plt.title('RSI for First Solar Stock');
plt.ylabel('RSI Normalized');
plt.xlabel('Trading days since start the date');
plt.legend(loc = 'best');


# In[ ]:




# In[ ]:




# In[ ]:


