from collections import Counter, defaultdict

data = "../corpus/43000.txt"
file = open(data, encoding="utf-8").readlines()
# punct_list = ["》", ",", "。", "，", "；"]
counts = Counter()
for line in file:
    line = line.rstrip().split("\t")
    nps = line[0].split()
    # if nps[-1] in punct_list:
    #     nn = nps[-2]
    # else:
    nn = nps[-1]
    classifier = line[-1]
    if nn not in counts.keys():
        counts[nn] = Counter()
    counts[nn][classifier] += 1
    # if classifier not in counts.keys():
    #     counts[classifier] = Counter()
    # counts[classifier][nn] += 1

ge_counts = defaultdict(list)
for key, dict in counts.items():
    if "个" in dict.keys():
        lst = sorted(dict.items(), key=lambda x: x[1], reverse=True)
        for tup in lst:
            if key not in ge_counts.keys():
                ge_counts[key] = []
            ge_counts[key].append(tup[0])
            if tup[0] == "个":
                break

# print(ge_counts)
# print(counts)

out = open('bidirectional.txt', 'w', encoding='utf-8')
for line in file:
    line = line.rstrip().split("\t")
    nps = line[0].split()
    # if nps[-1] in punct_list:
    #     nn = nps[-2]
    # else:
    nn = nps[-1]
    classifier = line[1]
    classifier_lst = []
    if nn in ge_counts.keys() and classifier in ge_counts[nn]:
        classifier_lst.append(classifier)
        for cl in ge_counts[nn]:
            if cl != classifier:
                classifier_lst.append(cl)
        out.write("%s\t%s\n" %(line[0], " ".join(classifier_lst)))
    else:
        out.write("%s\t%s\n" %(line[0], line[1]))
out.close()
