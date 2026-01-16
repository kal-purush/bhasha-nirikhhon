#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
lines_parse.py
~~~~~~~~~~

Parsing lines before feeding them into ML algorithms
"""

def char_range(c1, c2):
        l = []
        for c in xrange(ord(c1), ord(c2) + 1):
                l.append(chr(c))
        return l

def translate_feature(feature, voca_f):
    """Translate a feature with respect with its vocabulary."""
    if not len(voca_f):
        return feature
    if feature not in voca_f:
        raise ValueError("Problem in translate_feature, value not in vocabulary")
    return [int(feature == value) for value in voca_f]

def translate_line(line, voca):
    """Translate one line with respect to the vocabulary."""
    res = line[:]
    offset = 0
    for idx, f in enumerate(line):
        if len(voca[idx]):
            new_columns = translate_feature(f, voca[idx])
            try:
                res[idx + offset + len(new_columns) - 1:] = line[idx:]
            except:
                print "Probleme in translate_line"
                print line
            res[idx + offset: idx + offset + len(new_columns)] = new_columns
            offset += len(new_columns) - 1
    return res

def translate_lines(lines, voca):
    """Translate all the lines of input data with respect to a vocabulary."""
    return [translate_line(l, voca) for l in lines]
        
def process_training_lines(lines, voca, throw_id=True, delim=','):
	"""Translate the row input and perform feature scaling."""
	lines = [l.split(delim) for l in lines]
    y = [float(l[len(l)-1]) for l in lines]
    lines = translate_lines(lines, voca)
    l0 = lines[0]
    sum_l = [0 for feature in l0]
    mean_l = [0 for feature in l0]
    for l in lines:
        for indx, f in enumerate(l):
            mean_l[indx] += float(f)
            sum_l[indx] += 1
    min_l = [feature for feature in l0]
    max_l = [feature for feature in l0]
    range_l = [0 for feature in l0]
    for l in lines:
        for i, f in enumerate(l):
            max_l[i] = max(max_l[i], f)
            min_l[i] = min(min_l[i], f)
    for i, me in enumerate(mean_l):
        mean_l[i] = mean_l[i] / sum_l[i]
        range_l[i] = max_l[i] - min_l[i]
    for i, l in enumerate(lines):
        for j, f in enumerate(l):
            if range_l[j] != 0:
                lines[i][j] = (lines[i][j] / mean_l[j]) / range_l[j]
	if throw_id:
		lines = [l[1:] for l in lines]
    return (lines, y, mean_l, range_l)
                
def process_test_lines(lines, voc, mean_l, range_l, throw_id=True, delim=','):
	"""Transform the testing lines. Usually not needed."""
	lines = [l.split(delim) for l in lines]
	lines = translate_lines(lines, voc)
	for i, l in enumerate(lines):
		for j, f in enumerate(l):
			if range_l(j) != 0:
				lines[i][j] = (lines[i][j] - mean_l[j]) / range_l[j]
	if throw_id:
		lines = [l[1:] for l in lines]
	return lines

def process_inputs(training_lines, test_lines, voc, throw_id=True, delim=','):
	"""Transform both the training lines and testing lines."""
	training_x, training_y, mean_l, range_l = process_training_lines(training_lines, voc, throw_id, delim)
	test_x = process_test_lines(test_lines, voc, mean_l, range_l, throw_id, delim)
	return training_x, training_y, test_x

def load_data(file_name, skip_header=True):
	""" Load the data from a file."""
	lines = []
	with open(file_name, 'r') as f:
		header = skip_header
		for line in f:
			if header:
				header = False
				continue
			line = line.rstrip('\n').rstrip('\r')
			lines.append(line)
	return lines

def split_cva(lines):
	""" Split the input into 3/4 and 1/4."""
	train = []
	test = []
	for l in lines:
		if random.randint(0, 3) == 0:
			test.append(l)
		else:
			train.append(l)
	return train, test

def load_data_and_split(file_name, skip_header=True, delim=','):
	""" Load data, split into testing set and cva, split the cva truth
	AND returns the set as string lists."""
	lines = load_data(file_name, skip_header)
	train_lines, test_lines = split_cva(lines)
	test_lines = [l.split(delim) for l in test_lines]
	testingTruth = [l[-1] for l in test_lines]
	testing_lines = [reduce(lambda x, y: x + ',' + y, l[:-1]) for l in testing_lines]
	return train_lines, testing_lines, testingTruth

# def load_data_and_split_cva(file_name, voc, skip_header=True, delim=',', 
# 							throw_id=True):
# 	""" Load data from file, preprocess it and split into training set
# 	and testing set."""
# 	x, y = load_data(file_name, voc, skip_header, delim, throw_id)
# 	train, test = split_cva(zip(x, y))
# 	train_x = [t[0] for t in train]
# 	train_y = [t[1] for t in train]
# 	test_x = [t[0] for t in test]
# 	test_y = [t[1] for t in test]
# 	return train_x, train_y, test_x, test_y

# def load_and_split(file_name, voc, skip_header=True, delim=',', throw_id=True):
# 	""" Load data from file, preprocesss it and split into training set,
# 	cva set and testing set. Thats the function you want to use."""
# 	x, y = load_data(file_name, voc, skip_header, delim, throw_id)
# 	train, test = split_cva(zip(x, y))
# 	train, cva = split_cva(train)
# 	train_x = [t[0] for t in train]
# 	train_y = [t[1] for t in train]
# 	test_x = [t[0] for t in test]
# 	test_y = [t[1] for t in test]
# 	cva_x = [t[0] for t in cva]
# 	cva_y = [t[1] for t in cva]
# 	return train_x, train_y, cva_x, cva_y, test_x, test_y
	
	
