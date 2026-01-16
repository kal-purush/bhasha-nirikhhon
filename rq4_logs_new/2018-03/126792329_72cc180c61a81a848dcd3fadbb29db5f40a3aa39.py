import pandas as pd
import quandl
import numpy as np
import matplotlib.pyplot as plt

quandl.ApiConfig.api_key = "KNFp8d4xrmLSxGBQst4Y"


#Returns a dataframe of a ticker with columns consisting of: date, adjusted close, and the simple moving average.
#Parameters: ticker(string: e.g.('AAPL')); start/enddate(string: e.g.('2017-12-14')); window for the moving average(integer: e.g. (10 for 10-day SMA, 100 for 100-day SMA ,etc.))
def SMA(ticker, startdate, enddate, window):
	data = quandl.Dataset('WIKI/' + ticker).data(params={'start_date':startdate, 'end_date':enddate})
	data = data.to_pandas().reset_index()
	data = data[['Date', 'Adj. Close']]

	closes = data['Adj. Close'].tolist()

	currwindow = []
	averages = []

	for close in closes:
		if len(currwindow) < (window - 1):
			currwindow.append(close)
			averages.append(np.nan)
			continue
		currwindow.append(close)
		avg = sum(currwindow) / window
		averages.append(avg)
		currwindow.pop(0)

	data[str(window) + '-days SMA'] = averages

	return data