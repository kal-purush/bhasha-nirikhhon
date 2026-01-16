# coding=utf-8
# создано сотрудниками АО "НИИЧаспром"
# Данная программа предназначена для анализа надежности радиоэлектронных модулей, выполн
# !pip install brewer2mpl
#
# Import Data

import matplotlib as mpl
import numpy as np
import matplotlib.pyplot as plt

VBR = np.load("VBR.npy")
T = np.load("T.npy")
Z = np.load("Z.npy")
# series1 = np.array([3,4,5,3])
# series2 = np.array([1,2,2,5])
# series3 = np.array([2,3,3,4])

index = np.arange(len(T))

#E plt.axis([-0.5, 3.5, 3])
plt.title('A Multiseries Stacked Bar Chart')
for i in range(len(T)):
    index[i]=index[i]
#   plt.bar(index,VBR[i,0],color='r')
#   print VBR[i,0]
    plt.bar(index[i],VBR[i,3],color='b')
    plt.bar(index[i],VBR[i,9],color='g')
 #   print index


# plt.xticks(index,['Jan18','Feb18','Mar18','Apr18'])
plt.show()