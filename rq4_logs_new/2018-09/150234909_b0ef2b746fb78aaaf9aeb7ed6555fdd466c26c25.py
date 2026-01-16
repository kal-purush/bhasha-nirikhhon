import numpy as np
import pandas as pd
import math

def Hesse(theta, data, t):
    e = math.e
    sum = 0
    for i in range(t+1):
        a = np.array(data.iloc[i])
        sum += (e ** (np.dot(theta.T, a))) * (np.dot(np.matrix(a).T, np.matrix(a))) / (1 + e ** (np.dot(theta.T, a))) ** 2
    return sum

def Gradient(theta, data, t):
    e = math.e
    sum = 0
    for i in range(t+1):
        a = np.array(data.iloc[i])
        sum += (e ** (np.dot(theta.T, a))) * (a) / (1 + e ** (np.dot(theta.T, a)))
    return sum

# data = hogehoge
sigma = 0.5 ** 2
d = 5
theta = np.array([0 for i in range(d)])

for t in range(0, 144):
    a = np.array(data.iloc[t])
    print(a)
    while True:
        H_t = np.eye(d) / sigma + Hesse(theta, data, t)
        G_t = theta / sigma + Gradient(theta, data, t)
        theta_tmp = theta - np.dot(np.linalg.inv(H_t), G_t)
        theta_tmp = np.ravel(theta_tmp)
        if np.allclose(theta, theta_tmp):
            break
        theta = theta_tmp
        print (theta, theta_tmp)

    print("======================================================")