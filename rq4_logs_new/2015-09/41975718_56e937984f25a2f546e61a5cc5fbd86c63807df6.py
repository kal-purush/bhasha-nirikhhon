#!/bin/python
import csv
import numpy as np
import matplotlib.pyplot as plt


with open('air_data.csv', 'rb') as csvfile:
	reader = csv.reader(csvfile, delimiter = '\t', quotechar = '|')

	comment = reader.next()
	headers = reader.next()

	columns = {}
	for header in headers:
		columns[header] = []
	
	for row in reader:
		for header, value in zip(headers, row):
			columns[header].append(value)

	

X = np.arange(0, 300)
Y = columns['NO'][0:300]

line, = plt.plot(X, Y)

plt.show()