# -*- coding: utf-8 -*-
"""
# project: AdaBoost algrithom with decision tree
# author:  xhj
# email:   1124418652@qq.com
# date:    2018 8/23
"""

import random
import numpy as np 
import matplotlib.pyplot as plt
from math import inf, log, exp  
from collections import namedtuple

def create_dataSet(dim = 3, num = 10):
	dataSet = np.zeros((num, dim))
	labels = np.ones((num))
	
	for i in range(num):
		labels[i] = random.randint(0, 1)
		if labels[i] == 0: labels[i] = -1
		for j in range(dim):
			dataSet[i][j] = random.randint(1,10)
	# print(dataSet,"\n", labels,  np.ones((num))*(1/num))
	return dataSet, labels, np.ones((num))*(1/num)

def stump_classify(dataSet, dim, thresold, ineq):
	res_array = np.ones(len(dataSet))
	
	if ineq == 'lt':
		res_array[dataSet[:, dim] <= thresold] = -1 
	else:
		res_array[dataSet[:, dim] > thresold] = -1
	# print(res_array)
	return res_array

def build_stump(dataSet, labels, Darray):
	data_matrix = np.mat(dataSet)
	# label_matrix = np.mat(labels).T
	num, dim = data_matrix.shape
	min_error = inf
	best_stump = namedtuple("best_stump", ["dim", "thresold", "ineq", "min_error",\
											"classify_array"])

	# print("dataSet:\n", dataSet, "\nlabels:\n", labels)

	for i in range(dim):
		min_val = data_matrix[:, i].min()
		max_val = data_matrix[:, i].max()
		step = 2 * num
		step_size = (max_val - min_val) / step
		
		for j in range(-1, int(step) + 1):
			thresold = min_val + j * step_size

			for ineq in ['lt', 'gt']:
				classify_array = stump_classify(dataSet, i, thresold, ineq)
				error_array = np.zeros(num)
				error_array[classify_array != labels] = 1 
				# print("dataSet:\n", dataSet, "\nlabels:\n", labels)
				# print("dim:%s\t" %(dim), "thresold:%s\t" %thresold, "ineq:%s\n" %ineq,\
				# 	"error_array", error_array)
				error_values = (error_array * Darray).sum()
				
				if error_values < min_error:
					min_error = error_values
					best_stump.dim = i
					best_stump.thresold = thresold
					best_stump.ineq = ineq
					best_stump.min_error = min_error
					best_stump.classify_array = classify_array
	return best_stump

def adaBoost(dataSet, labels, Darray, numIt = 40):
	week_classify_set = []
	accumulate_classify = np.zeros(len(labels))

	for i in range(numIt):
		error_count = np.ones(len(labels))
		best_stump = build_stump(dataSet, labels, Darray)
		alpha = 0.5 * log((1 - best_stump.min_error) / max(best_stump.min_error, 1e-16))
		best_stump.alpha = alpha
		week_classify_set.append(best_stump)
		print("dim:%s\t" %best_stump.dim, "thresold:%s\t" %best_stump.thresold,\
			"ineq:%s\t" %best_stump.ineq, "min_error:%s\t" %best_stump.min_error,\
			"alpha:%s\n" %best_stump.alpha, "classify_array:%s\t" %best_stump.classify_array)
		
		for index, d in enumerate(Darray):
			Darray[index] = d * exp(-best_stump.alpha * \
									best_stump.classify_array[index] * labels[index])
		
		Darray = Darray / Darray.sum()
		# print(Darray)
		accumulate_classify += best_stump.alpha * best_stump.classify_array
		final_classify = np.sign(accumulate_classify)
		error_count[final_classify == labels] = 0
		# print(final_classify)

		if error_count.sum() == 0.0:
			print("duplicate times: %s\n" %i, "total error: %s\n" %(error_count.sum()/len(labels)))
			return week_classify_set
			break
	print("duplicate times: %s\n" %i, "total error: %s\n" %(error_count.sum()/len(labels)))
	return week_classify_set

def main():
	dataSet, labels, Darray = create_dataSet(3, 40)
	stump_classify(dataSet, 1, 3, 'lt')
	classify_set = adaBoost(dataSet, labels, Darray, 100)
	

if __name__ == '__main__':
	main()