# Julia's source Python file :)

# import yahoo finance data
import yfinance as yf
# import stockstats data
from stockstats import StockDataFrame as ss

# import necessary libraries
import matplotlib as mp
import numpy as np
import pandas as pd
import pytz
import warnings
import time
import random
import statistics

# localize time - ambiguous error
#tz = pd.Timestamp('2023-11-05')
#tz.tz_localize(pytz.timezone('US/Pacific'))

# download data for apple stock
apple_ticker = yf.Ticker("AAPL")
# download data from past 20 years
apple_data = yf.download("AAPL", start = '2004-01-01', interval = '1d')
apple_df = ss.retype(apple_data)
# introduce stochrsi, macd, mfi
apple_data[['stochrsi', 'macd', 'mfi']] = apple_df[['stochrsi', 'macd', 'mfi']]
print(apple_data)

# temporary: working with info
#print(apple_ticker.balance_sheet)






