# -*- coding: utf-8 -*-
"""
# project: decision tree with C4.5
# author:  xhj
# email:   1124418652@qq.com
# date:    2018 8/20
"""

import operator
import numpy as np 
from math import log
import matplotlib.pyplot as plt

def split_dataSet(dataSet, axis, value):
	
	return_set = []

	for line in dataSet:
		if line[axis] == value:
			tmp = line[:axis]
			tmp.extend(line[axis+1:])
			return_set.append(tmp)

	return return_set

def cal_sannon_ratio(dataSet):

	dataSet = np.array(dataSet)
	length = len(dataSet)
	label_count = {}

	for label in dataSet[:, -1]:
		if label not in label_count.keys():
			label_count[label] = 1 
		else:
			label_count[label] += 1 

	sannon = 0.0
	for key in label_count.keys():
		propertion = label_count[key] / length
		sannon -= propertion * log(propertion, 2)

	return sannon

def choose_best_feature(dataSet):

	global_entropy = cal_sannon_ratio(dataSet)
	feature_num = len(dataSet[0]) - 1 
	condition = {}
	info_gain = 0.0

	for feature in range(feature_num):
		values = [x[feature] for x in dataSet]
		values = set(values)

		tmp_entropy = 0.0
		HA = 0.0

		for value in values:
			sub_dataSet = split_dataSet(dataSet, feature, value)
			feature_ratio = len(sub_dataSet) / len(dataSet)
			condition_entropy = cal_sannon_ratio(sub_dataSet)
			HA -= feature_ratio * log(feature_ratio, 2)
			tmp_entropy += feature_ratio * condition_entropy

		tmp_gain = (cal_sannon_ratio(dataSet) - tmp_entropy) / HA 
		if tmp_gain > info_gain:
			info_gain = tmp_gain
			best_feature = feature 

	return best_feature

def majorityCnt(class_list):
	class_dict = {}
	for i in class_list:
		if i not in class_dict.keys():
			class_dict[i] = 1
		else:
			class_dict[i] +=1 
	sorted_class = sorted(class_dict.items(),\
							key = operator.itemgetter(1), reverse = True)
	return sorted_class[0][0]

def create_tree(dataSet, labels):
	class_list = [example[-1] for example in dataSet]

	if class_list.count(class_list[0]) == len(dataSet):
		return class_list[0]

	elif len(dataSet[0]) == 1:
		return majorityCnt(class_list)

	best_feature = choose_best_feature(dataSet)
	best_label = labels[best_feature]
	del(labels[best_feature])
	tree = {best_label:{}}

	values = [example[best_feature] for example in dataSet]
	values = set(values)

	for value in values:
		sub_dataSet = split_dataSet(dataSet, best_feature, value)
		tmp_label = labels[:]
		tree[best_label][value] = create_tree(sub_dataSet, tmp_label)

	return tree

def getNumLeaves(tree):
	numLeaves = 0
	firstStr = list(tree.keys())[0]
	secondDict = tree[firstStr]

	for key in secondDict.keys():
		if type(secondDict[key]).__name__ == 'dict':
			numLeaves += getNumLeaves(secondDict[key])
		else:
			numLeaves += 1
	return numLeaves

def getTreeDepth(tree):
	maxDepth = 0;
	firstStr = list(tree.keys())[0]
	secondDict = tree[firstStr]

	for key in secondDict.keys():
		if type(secondDict[key]).__name__ == 'dict':
			thisDepth = 1 + getTreeDepth(secondDict[key])
		else:
			thisDepth = 1 
		if thisDepth > maxDepth: maxDepth = thisDepth

	return maxDepth

def draw(tree):
	leaves = getNumLeaves(tree)
	depth = getTreeDepth(tree)
	fig = plt.figure()
	fig.clf()
	ax = fig.add_subplot(111, frameon = False)
	ax.set_xlim(0, leaves+2)
	ax.set_ylim(0, depth+2)
	for i in range(depth):
		ax.annotate("%s" %(list(tree.keys())[0]), xy=(leaves, depth))
	plt.show()
	
def main():
	dataSet = [[1,1,1,'yes'],
			   [2,1,1,'yes'],
			   [3,1,0,'no'],
			   [1,0,1,'no'],
			   [1,0,1,'no'],
			   [2,2,2,'yes'],
			   [4,1,2,'no'],
			   [4,2,0,'yes'],
			   [2,1,3,'no'],
			   [3,1,2,'yes'],
			   [1,4,0,'no']]
	labels = ['no surfacing', 'flippers', 'water']

	# dataSet = [[1,1,'yes'],
	# 		   [1,1,'yes'],
	# 		   [1,0,'no'],
	# 		   [0,1,'no'],
	# 		   [0,1,'no']]
	# labels = ['no surfacing', 'flippers']

	tree = create_tree(dataSet, labels)
	print(tree)
	print(getNumLeaves(tree))
	print(getTreeDepth(tree))
	draw(tree)

if __name__ == '__main__':
	main()