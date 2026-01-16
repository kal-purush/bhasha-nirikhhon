# in the root directory of this repository, run
# > PYTHONPATH=. python testCodes/test1.py


wordDict = {'_' : 0, '-' : 1}
fin1 = open('datasets/en-valid-10k/qa1_train.txt')
fin2 = open('datasets/en-valid-10k/qa19_train.txt')

from utils.tools import *
import numpy as np

for case in fin1.readlines():
    lineID, questions, answers = lineToValidLists(case, wordDict)
    print(lineID, questions, answers)
    print(getOneHots(np.array(questions), len(wordDict)))


print('wordDict :\t', wordDict)
