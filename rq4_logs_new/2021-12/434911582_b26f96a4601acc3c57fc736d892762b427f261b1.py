from sklearn import datasets
import matplotlib.pyplot as plt


def loadDataSet(fileName, splitChar='\t'):
    dataSet = []
    with open(fileName) as fr:
        for line in fr.readlines():
            curline = line.strip().split(splitChar)
            fltline = list(map(float, curline))
            dataSet.append(fltline)
    return dataSet


dataset = loadDataSet('DBSCANpoints.txt', splitChar=',')
x = []
y = []
for data in dataset:
    x.append(data[0])
    y.append(data[1])
plt.figure(figsize=(8, 6), dpi=80)
plt.scatter(x,y)
plt.show()