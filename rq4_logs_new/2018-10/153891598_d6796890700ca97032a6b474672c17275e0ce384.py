

import numpy as np
import matplotlib.pyplot as plt
import math

def main():
    xhat=[]
    res=[]
    myArr = np.array([
         [0.697, 0.460, 1],
         [0.774, 0.376, 1],
         [0.634, 0.264, 1],
         [0.608, 0.318, 1],
         [0.556, 0.215, 1],
         [0.403, 0.237, 1],
         [0.481, 0.149, 1],
         [0.437, 0.211, 1],
         [0.666, 0.091, 0],
         [0.243, 0.267, 0],
         [0.245, 0.057, 0],
         [0.343, 0.099, 0],
         [0.639, 0.161, 0],
         [0.657, 0.198, 0],
         [0.360, 0.370, 0],
         [0.593, 0.042, 0],
         [0.719, 0.103, 0]
         ])
    for arr in myArr:
        xhat.append([float(1.0),float(arr[0]),float(arr[1])])
        res.append([int(arr[2])])
    return xhat,res





def y(x):
    return 1.0/(1+np.exp(-x))


def tidu(dataMat, labelMat):
    m, n = np.shape(dataMat)
    alpha = 0.1
    maxCycles = 500
    weights = np.array(np.ones((n, 1)))

    for k in range(maxCycles):
        a = np.dot(dataMat, weights)
        h = y(a)
        error = (labelMat - h)
        weights = weights + alpha * np.dot(np.array(dataMat).transpose(), error)
    return weights


def pic(weights):
    xhat,res=main()
    dataArr=np.array(xhat)#dataArr为17*3
    res=np.array(res)
    n=np.shape(dataArr)[0]   #n为17
    xcord1=[]
    xcord2=[]
    ycord1=[]
    ycord2=[]
    for i in range(n):
        if int(res[i]==1):        #好瓜的密度和含糖率
            xcord1.append(dataArr[i,1])
            ycord1.append(dataArr[i,2])
        else:                     #坏瓜的密度和含糖量
            xcord2.append(dataArr[i,1])
            ycord2.append(dataArr[i,2])
    fig=plt.figure()
    ax=fig.add_subplot(111)
    ax.scatter(xcord1, ycord1, s=30, c='red', marker='+')  #好瓜红点
    ax.scatter(xcord2, ycord2, s=30, c='green', marker='+')  # 坏瓜绿点
    x=np.arange(0.2,0.8,0.1)
    y=(-weights[0]-weights[1]*x)/weights[2]
    ax.plot(x,y)
    plt.xlabel('x1')
    plt.ylabel('x2')
    plt.show()



dataArr,labelMat=main()
print(np.array(dataArr))
print(np.array(labelMat))
print(tidu(dataArr,labelMat))
print("show the result...\t",pic(tidu(dataArr,labelMat)))