import urllib
import time
import datetime
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
from matplotlib.finance import candlestick_ochl as candle
import matplotlib
matplotlib.rcParams.update({'font.size': 9})


eachStock = 'TSLA'

def movingaverage(values, window):
	weights = np.repeat(1.0, window) / window
	smas = np.convolve(values, weights, 'valid')
	return smas

def graphData(stock, MA1, MA2):
	# try:
		try:
			print('Pulling data on', stock)
			urlToVisit = 'http://chartapi.finance.yahoo.com/instrument/1.0/'+stock+'/chartdata;type=quote;range=3y/csv'
			stockFile = []
			try:
				sourceCode = urllib.request.urlopen(urlToVisit).read().decode('utf-8')
				splitSource= sourceCode.split('\n')
				for eachLine in splitSource:
					splitLine = eachLine.split(',')
					if len(splitLine) == 6:
						if 'values' not in eachLine:
							stockFile.append(eachLine)


			except Exception as e:
				print(str(e), 'Failed to orginize pulled data')



		except Exception as e:
			print(str(e), 'failed to pull price data')
		
		date, closep, highp, lowp, openp, volume = np.loadtxt(stockFile, delimiter=',', unpack=True,
												   converters = {0: mdates.bytespdate2num('%Y%m%d')})
		
		x = 0
		y = len(date)
		candleAr = []
		while x < y:
			appendLine = date[x], openp[x], closep[x], highp[x], lowp[x], volume[x]
			candleAr.append(appendLine)
			x += 1

		Av1 = movingaverage(closep, MA1)
		Av2 = movingaverage(closep, MA2)

		SP = len(date[MA2-1:])

		label1 = str(MA1) + ' SMA'
		label2 = str(MA2) + ' SMA'


		fig = plt.figure(facecolor='black')
		ax1 = plt.subplot2grid((5,4), (0,0), rowspan=4, colspan=4, facecolor='#07000d')
		candle(ax1, candleAr, width=0.75, colorup='#9eff15', colordown='#ff1717')

		ax1.plot(date[-SP:], Av1[-SP:], 'red', label = label1, linewidth=1.5)
		ax1.plot(date[-SP:], Av2[-SP:])

		# ax1.plot(date2, openp)
		# ax1.plot(date2, highp)
		# ax1.plot(date2, lowp)
		# ax1.plot(date2, closep)
		plt.ylabel('Stock price')
		plt.legend(loc=3, fancybox=True, prop={'size':7})
		ax1.grid(True, color='w', ls='dotted')
		ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
		ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
		ax1.yaxis.label.set_color('w')
		ax1.spines['bottom'].set_color('#5998ff')
		ax1.spines['top'].set_color('#5998ff')
		ax1.spines['left'].set_color('#5998ff')
		ax1.spines['right'].set_color('#5998ff')
		ax1.tick_params(axis='y', colors='w')

		# volumeMin = volume.min()
		volumeMin = 0

		ax2 = plt.subplot2grid((5,4), (4,0), sharex=ax1, rowspan=1, colspan=4, facecolor='#07000d')
		ax2.plot(date, volume, '#00ffe8', linewidth=.80)
		ax2.fill_between(date, volumeMin, volume, facecolor='#00ffe8', alpha=.5)
		ax2.axes.yaxis.set_ticklabels([])
		plt.ylabel('Volume', color='w')
		ax2.grid(False)
		ax2.spines['bottom'].set_color('#5998ff')
		ax2.spines['top'].set_color('#5998ff')
		ax2.spines['left'].set_color('#5998ff')
		ax2.spines['right'].set_color('#5998ff')
		ax2.tick_params(axis='x', colors='w')
		ax2.tick_params(axis='y', colors='w')







		for label in ax1.xaxis.get_ticklabels():
			label.set_rotation(90)


		for label in ax2.xaxis.get_ticklabels():
			label.set_rotation(45)


		plt.subplots_adjust(left=.10, bottom=.14, right=.94, top=.95, wspace=.20, hspace=.07)

		
		plt.suptitle(stock + ' Stock Price', color='w')

		plt.setp(ax1.get_xticklabels(), visible=False)

		plt.subplots_adjust(left=.09, bottom=.18, right=.94, top=.94, wspace=.20, hspace=0)

		plt.show()
		fig.savefig(eachStock + '.png', facecolor=fig.get_facecolor())

	# except Exception as e:
	# 	print('failed main loop', str(e))


graphData('TSLA', 50, 200)