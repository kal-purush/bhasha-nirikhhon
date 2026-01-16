"""
theory refer to "Fast Training of Support Vector Machines using Sequential Minimal Optimization"
structure refer to "Mashion Learning in action"

"""

import numpy as np
import matplotlib
import random
import matplotlib.pyplot as plt
def load_dataset(fileName):
    dataMat = []
    lableMat = []
    fr = open(fileName)
    for line in fr.readlines():
        data = line.strip().split('\t')
        dataMat.append([float(data[0]),float(data[1])])
        lableMat.append(int(data[2]))
    return dataMat,lableMat
def judge_bound(L,H,alpha):
    if L >= alpha:
        alpha = L
    elif H <= alpha:
        alpha = H
    return alpha 

def diff_j(i,m):
    j = i
    while(j==i):
        j = int(random.uniform(0,m))
    return j


def simple_svm(dataset,lable,C,maxiter):
    iter =0 
    dataset = np.mat(dataset)
    lable = np.mat(lable).T
    m = dataset.shape[0]
    alpha = np.zeros((m,1))
    b = 0
    while (iter<maxiter):
        alphachange = 0
        for i in range(m):
            j = diff_j(i,m)
            alphaiold = alpha[i].copy()
            alphajold = alpha[j].copy()
            fxi = float(np.multiply(alpha,lable).T*(dataset*dataset[i,:].T)) + b
            fxj = float(np.multiply(alpha,lable).T*(dataset*dataset[j,:].T)) + b
            Ei = -lable[i] + fxi
            Ej = -lable[j] + fxj
            if (lable[i]!=lable[j]):
                L = max(0,alphajold-alphaiold)
                H = min(C,C+alphajold-alphaiold)
            else:
                L=max(0,alphajold+alphaiold-C)
                H=min(C,alphajold+alphaiold)
            if L == H:
                continue
            eta = dataset[i,:]*dataset[i,:].T +  dataset[j,:]*dataset[j,:].T  - 2*dataset[i,:]*dataset[j,:].T
            if eta <=0:
                continue
            alpha[j] = alphajold + (Ei-Ej)*lable[j]/eta
            alpha[j] = judge_bound(L,H,alpha[j])
            if abs(alpha[j] - alphajold) <0.0000001:
               continue
            alpha[i] = alphaiold + lable[i]*lable[j]*(alphajold - alpha[j])
            b1  =b - Ei - lable[i]*(alpha[i] - alphaiold)*dataset[i,:]*dataset[i,:].T \
                -lable[j]*(alpha[j] - alphajold)*dataset[i,:]*dataset[j,:].T
            b2  =b - Ej - lable[i]*(alpha[i] - alphaiold)*dataset[i,:]*dataset[j,:].T \
                -lable[j]*(alpha[j] - alphajold)*dataset[j,:]*dataset[j,:].T

            if (0<alpha[i]) and (C>alpha[i]):
                b= b1
            elif  (0<alpha[j]) and (C>alpha[j]):
                b = b2
            else:
                b = (b1+b2)/2.0
            alphachange += 1
            #print("iter: %d i:%d, pairs changed %d" % (iter,i,alphachange))
        if alphachange == 0:
            iter = iter + 1
        else:
            iter = 0
        print ("iteration number: %d" % iter)
    return alpha,b


def plot_bestFit(alpha,b):
    """
    draw the result
    """
    dataset , lable = load_dataset('testSet.txt')
    x_plt_p = []
    y_plt_p = []

    x_plt_n = []
    y_plt_n = []
    for i in range(len(lable)):
        if lable[i] == 1:
            x_plt_p.append(dataset[i][0])
            y_plt_p.append(dataset[i][1])
        else:
            x_plt_n.append(dataset[i][0])
            y_plt_n.append(dataset[i][1])
    plt.subplot(211)
    plt.scatter(x_plt_p, y_plt_p, c='red', s=30, marker='s')
    plt.scatter(x_plt_n, y_plt_n, c='blue', s=30)

    dataMatrix = np.mat(dataset); labelMat = np.mat(lable).transpose()
    w = np.multiply(alpha,labelMat).T*dataMatrix
    w=w.getA()
    x = np.arange(-2,10,0.1)
    w=w[0]
    b=b.getA()
    b=b[0]
    y = -(b+w[0]*x)/w[1]
    plt.plot(x,y)

    plt.show()





      