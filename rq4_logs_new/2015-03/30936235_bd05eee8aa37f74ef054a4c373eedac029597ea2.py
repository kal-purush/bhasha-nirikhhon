# build input files for translation system comparison with libsvm

import sys
from extract import extract, feats2str

if len(sys.argv) < 5:
	sys.stderr.write("usage: ./merge_for_comparison sys1 sys2 ref label [binarization, default=false]\n")
	sys.exit(1)

sys1 = open(sys.argv[1])
sys2 = open(sys.argv[2])
ref = open(sys.argv[3])
lab = open(sys.argv[4])
line1 = sys1.readline()
line2 = sys2.readline()
rline = ref.readline()
label = lab.readline()
if len(sys.argv) < 6 or sys.argv[5].lower == "false":
	while line1 != "" and line2 != "" and rline != "" and label != "":
		feats1 = extract(line1, rline)
		feats2 = extract(line2, rline)
		feats = {}
		for k in feats1.keys():
			feats[k] = feats1[k]
		for k in feats2.keys():
			feats[k + 33] = feats2[k]
		sys.stdout.write(label.strip() + " " + feats2str(feats) + "\n")
		line1 = sys1.readline()
		line2 = sys2.readline()
		rline = ref.readline()
		label = lab.readline()
else:
	# collect all possible values
	feats = {}
	linen = 0
	while line1 != "" and line2 != "" and rline != "":
		if linen % 1000 == 0:
			sys.stderr.write(".")
		feats1 = extract(line1, rline)
		feats2 = extract(line2, rline)
		for k in feats1.keys():
			if feats.get(k, []) == []:
				feats[k] = [feats1[k]]
			elif not feats1[k] in feats[k]:
				feats[k].append(feats1[k])
		for k in feats2.keys():
			if feats.get(k + 33, []) == []:
				feats[k + 33] = [feats2[k]]
			elif not feats2[k] in feats[k + 33]:
				feats[k + 33].append(feats2[k])
		line1 = sys1.readline()
		line2 = sys2.readline()
		rline = ref.readline()
		linen += 1
	sys1.close()
	sys2.close()
	ref.close()

	sys1 = open(sys.argv[1])
	sys2 = open(sys.argv[2])
	ref = open(sys.argv[3])
	# convert feature to index
	feat2index = {}
	index = 1
	for k in feats.keys():
		for v in feats[k]:
			feat2index[(k, v)] = index
			index += 1

	# output binarized feature
	line1 = sys1.readline()
	line2 = sys2.readline()
	rline = ref.readline()
	while line1 != "" and line2 != "" and rline != "" and label != "":
		feats1 = extract(line1, rline)
		feats2 = extract(line2, rline)
		bifeats = {}
		for k in feats1.keys():
			bifeats[feat2index[(k, feats1[k])]] = 1.0
		for k in feats2.keys():
			bifeats[feat2index[(k + 33, feats2[k])]] = 1.0
		sys.stdout.write(label.strip() + " " + feats2str(bifeats) + "\n")
		line1 = sys1.readline()
		line2 = sys2.readline()
		rline = ref.readline()
		label = lab.readline()
sys1.close()
sys2.close()
ref.close()
lab.close()

