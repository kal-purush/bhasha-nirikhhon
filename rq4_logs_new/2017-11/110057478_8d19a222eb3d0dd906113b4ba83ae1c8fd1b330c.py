'''
Naives Bayes Classifier
Author: David Iliaguiev
		iliaguievdavid@gmail.com
'''

from numpy import *
import csv
from collections import OrderedDict
import numpy as np
import matplotlib.pyplot as plt

filename = 'mushrooms.csv'
with open(filename, 'rb') as raw_file:
    raw_data = csv.reader(raw_file, delimiter=',', quoting=csv.QUOTE_NONE)
    data_list = list(raw_data)


ndims = len(data_list[0])
npts = len(data_list)

char_maps = [OrderedDict() for i in range(ndims)]
reverse_maps = [[] for i in range(ndims)]
data_mat = empty((npts,ndims),dtype=int32)
for i,cdata in enumerate(data_list):
    for j,cstr in enumerate(cdata):
        if cstr not in char_maps[j]:
            char_maps[j][cstr] = len(char_maps[j])
            reverse_maps[j].append(cstr)
        data_mat[i,j] = char_maps[j][cstr]
del data_list

random.seed(0)
data_perm = random.permutation(npts)
data_train = data_mat[data_perm[0:(8*npts/10)],:]
data_test = data_mat[data_perm[(8*npts/10):],:]
data_ranges = data_mat[:,1:].max(axis=0)

e_count = 0
p_count = 0

for i in data_train:
    if (i[0]) == 0:
        e_count += 1
    elif (i[0]) == 1:
        p_count += 1
print('# {}, Fraction of edible mushrooms in training data: {}%'.format(e_count, int(100*(e_count/float(len(data_train))))))
print('# {}, Fraction of poisonous mushrooms in training data: {}%'.format(p_count, int(100*(p_count/float(len(data_train))))))



# graph the histograms
def graph(j, po_hist_arg, ed_hist_arg, feature, bins):
    weights_po = ones_like(po_hist_arg) / float(len(po_hist_arg))
    plt.subplot(211)
    plt.hist(po_hist_arg, weights=weights_po)
    plt.xticks(range(0, bins + 1 ))
    plt.title("Feature {}:{}, Poisonous".format(j, feature))
    plt.ylabel("Frequency")
    plt.subplot(212)
    weights_ed = ones_like(ed_hist_arg) / float(len(ed_hist_arg))
    plt.hist(ed_hist_arg, weights=weights_ed)
    plt.xticks(range(0, bins + 1))
    plt.title("Feature {}:{}, Edible".format(j, feature))
    plt.ylabel("Frequency")
    plt.show()

edible_data = [ i for i in data_train if i[0] == 0]
poisonous_data = [ i for i in data_train if i[0] == 1]

features = ["Cap-Shape", "Cap-Surface", "Cap-Color", "Bruises", "Odor", "Gill-Attachment", "Gill-Spacing", "Gill-Size", "Gill-Color",
            "Stalk-Shape", "Stalk-Root", "Stalk-Surface-Above-Ring", "Stalk-Surface-Below-Ring", "Stalk-Color-Above-Ring", "Stalk-Color-Below-Ring",
            "Veil-Type", "Veil-Color", "Ring-Number", "Ring-Type", "Spore-Print-Color", "Population", "Habitat"]


edible_hist = list()
poison_hist = list()

for i in range(ndims - 1):
	etemp = [j[i+1] for j in edible_data]
	ptemp = [j[i+1] for j in poisonous_data]
	edible_hist.append(etemp)
	poison_hist.append(ptemp)

# this is the graph uncomment it later

for i in range(ndims-1):
	graph(i+1, poison_hist[i], edible_hist[i], features[i], data_ranges[i])


# priors
pe1 = float(p_count)/float(len(data_train)) # prior p(E=1)
pe0 = float(e_count)/float(len(data_train)) # prior p(E=0)

# fill matrix 
e_matrix = list()
p_matrix = list()
for i in range(ndims-1):
	probe = list()
	probp = list()
	for j in range(data_ranges[i] + 1):
		probe.append(edible_hist[i].count(j))
		probp.append(poison_hist[i].count(j))
	e_matrix.append(probe)
	p_matrix.append(probp)


# calculating theta_MAP for each feature and putting in 2D list, i.e p(F_i=f_i|E=1), p(F_i=f_i|E=0)
def calc_MAP(alpha):
	# calculating the posterior
	e_MAP = [[] for i in range(ndims-1)]
	p_MAP = [[] for i in range(ndims-1)]

	# used for biggest impact classifier
	bicv = []


	for i in range(ndims - 1):
		etheta = np.zeros(data_ranges[i] + 1)
		ptheta = np.zeros(data_ranges[i] + 1)
		bicvtemp = 0
		bicvtempabs = 0

		for j in range(data_ranges[i] + 1):
			etheta[j] = float(e_matrix[i][j] + alpha - 1)/float(e_count + (ndims-1)*alpha - (data_ranges[i] + 1))
			ptheta[j] = float(p_matrix[i][j] + alpha - 1)/float(p_count + (ndims-1)*alpha - (data_ranges[i] + 1))
			# for step 3 (if either theta 0 log(0) is undefined)
			if (etheta[j] != 0 and ptheta[j] != 0):
				bicvtemp = float(np.log(etheta[j]) - np.log(ptheta[j]))
				bicvtempabs = float(abs(np.log(etheta[j]) - np.log(ptheta[j])))
				bicv.append([i, j, bicvtemp, bicvtempabs])
		e_MAP[i] = etheta
		p_MAP[i] = ptheta
	return e_MAP, p_MAP, bicv

# calculating the posterior in log space using log_sum_exp shown in class notes and checking if the classification is correct
def calc_posterior(e_MAP, p_MAP, data_set):

	class_correct = 0

	for i in range(len(data_set)):
		# lp{1,0} = log(p(E)) + sum(from 1, to 22, p(f_i|E))
		lp0 = np.log(pe0)
		lp1 = np.log(pe1)
		for j in range(1, ndims-1):
			f_option = data_set[i][j]
			# check if there are occurences in which the option made it edible
			if e_MAP[j-1][f_option] != 0:
				lp0 += np.log(e_MAP[j-1][f_option])

			# check if there are occurences in which the option made it poisonous
			if p_MAP[j-1][f_option] != 0:
				lp1 += np.log(p_MAP[j-1][f_option])

		B = max(lp0,lp1)
		posterior = lp1 - (B + np.log(np.exp(lp1 - B) + np.exp(lp0 - B)))
		if posterior > np.log(0.5):
			if data_set[i][0] == 1:
				class_correct += 1
		elif posterior <= np.log(0.5):
			if data_set[i][0] == 0:
				class_correct += 1

	return class_correct

alpha = np.arange(1,2.01,0.01)

def best_alpha(alpha, data):
    best = 0
    best_a = 0
    all_alpha = np.zeros(len(alpha))
    size_data = len(data)
    i = 0
    for a in alpha:
        e_MAP, p_MAP, bicv = calc_MAP(a)
        amount_accuracy = float(calc_posterior(e_MAP, p_MAP, data))/size_data
        all_alpha[i] = amount_accuracy
        i += 1
        if amount_accuracy > best:
            best = amount_accuracy
            best_a = a
            best_bicv = bicv

    return best, best_a, all_alpha, bicv

# for step 2
accuracy_train, best_a_train, all_alpha_train, bicv_train = best_alpha(alpha, data_train)
print("best alpha: {}, accuracy for training data: {:2.2f}%".format(best_a_train, 100*accuracy_train))

accuracy_test, best_a_test, all_alpha_test, bicv_test = best_alpha(alpha, data_test)
print("best alpha {}, accuracy for test data: {:2.2f}%".format(best_a_test, 100*accuracy_test))

# for step 3
def sort_by_abs(item):
    return item[3]

def sort_by_val(item):
    return item[2]
abs_sorted = sorted(bicv_train, key=sort_by_abs)
raw_sorted = sorted(bicv_train, key=sort_by_val)

f = open('abs.txt', 'w')
f2 = open('raw.txt', 'w')
f.write('Asolute Biggest Impact on the classifier:\n')
for i in abs_sorted:
	f.write(str(i) + '\n')

f2.write('Regular(non-absolute) Biggest Impact on the classifier:\n')
for i in raw_sorted:
	f2.write(str(i) + '\n')

f.close()
f2.close()

plt.xlabel("Alpha values")
plt.ylabel("Probability")
plt.ylim((0.87, 1.00))
plt.xlim((1.0, 2.0))
plt.plot(alpha, all_alpha_train, label="Train Set")
plt.plot(alpha, all_alpha_test, label="Validation Set")
plt.legend()
plt.show()