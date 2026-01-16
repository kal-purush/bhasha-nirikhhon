import os
import copy

script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
rel_path = "Day14Input.txt"
abs_file_path = os.path.join(script_dir, rel_path)

with open(abs_file_path) as file:
    lines = file.readlines()
    lines = [line.rstrip() for line in lines]

#Process the input
seq = lines[0]
dictPairs = dict(list(map(lambda x: x.split(' -> '), lines[2:])))

#Makes all pairs
seqPairs = []
for j in range(len(seq) - 1):
    seqPairs.append((seq[j] + seq[j + 1]))

#Encounters of all numbers
seqDict = dict()
for s in seq:
    if s not in seqDict:
        seqDict[s] = 1
    else:
        seqDict[s] += 1

#Encounters of all pairs
seqPairDict = dict()
for s in seqPairs:
    if s not in seqPairDict:
        seqPairDict[s] = 1
    else:
        seqPairDict[s] += 1


for i in range(40):

    tempSeqPairDict = copy.deepcopy(seqPairDict)

    for pair, value in tempSeqPairDict.items():
        if pair in dictPairs and value > 0:
            
            seqPairDict[pair] -= tempSeqPairDict[pair]

            #Add new generated pairs
            newPair1 = pair[0] + dictPairs[pair]
            newPair2 = dictPairs[pair] + pair[1]

            if newPair1 not in seqPairDict:
                seqPairDict[newPair1] = value
            else:
                seqPairDict[newPair1] += value

            if newPair2 not in seqPairDict:
                seqPairDict[newPair2] = value
            else:
                seqPairDict[newPair2] += value

            #Add new generated values to final Dict
            if dictPairs[pair] not in seqDict:
                seqDict[dictPairs[pair]] = value
            else:
                seqDict[dictPairs[pair]] += value

print(max(seqDict.values()) - min(seqDict.values()))