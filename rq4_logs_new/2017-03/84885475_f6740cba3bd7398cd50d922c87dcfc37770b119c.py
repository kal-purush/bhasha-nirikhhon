from urllib.request import urlopen
import time

stocksToPull = 'AAPL', 'GOOG', 'GPRO'

def pullData(stock):
	try:
		fileLine = stock + '.txt'
		urlToVisit = 'http://chartapi.finance.yahoo.com/instrument/1.0/' + stock + '/chartdata;type=quote;range=1y/csv'
		sourceCode = urlopen(urlToVisit).read().decode('utf-8')
		splitSource = sourceCode.split('\n')

		for eachLine in splitSource:
			splitLine = eachLine.split(',')
			if len(splitLine) == 6:
				if 'values' not in eachLine:
					saveFile = open(fileLine, 'a')
					lineToWrite = eachLine + '\n'
					saveFile.write(lineToWrite)

		print('Pulled', stock)
		print('sleeping')
		time.sleep(5)

	except Exception as e:
		print('main loop', str(e))


pullData(stocksToPull)