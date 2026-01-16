from __future__ import division
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def warmUpExercise():
    print np.identity(5)

def plotData(input_file, x, y, presentation=False):
    df = pd.read_csv(input_file, names=[x, y])
    if presentation:
        fig = plt.figure()
    plt.scatter(df[x], df[y])
    plt.xlabel('Population')
    plt.ylabel('Profit')
    plt.show()
    if presentation:
        fig.suptitle('Profit By Population')
        fig.savefig('test.jpg')

def computeCost(input_file, x, y, theta):
    df = pd.read_csv(input_file, names=[x, y])
    m = len(df[y])
    df.insert(0,'Ones', pd.Series(np.ones(m)))
    h = pd.Series(np.dot(df.ix[:,'Ones':x], theta))
    sqrErrors = (h - df[y]) ** 2
    J = 1 / (2 * m) * sum(sqrErrors)
    print J

def gradientDescent(input_file, x, y, alpha, num_iters):
    df = pd.read_csv(input_file, names=[x, y])
    m = len(df[y])
    df.insert(0,'Ones', pd.Series(np.ones(m)))
    J_history = pd.Series(np.zeros(len(df)))
    X = df.ix[:,'Ones':x].T
    theta = pd.Series(np.zeros(len(df.columns) - 1))
    for i in range(num_iters):
        theta = theta - (alpha / m) * np.dot(X, pd.Series(np.dot(df.ix[:,'Ones':x], theta)) - df.ix[:, y])
    print theta

warmUpExercise()

plotData('uni_lin_reg_data.csv', 'Population', 'Profit')

computeCost('uni_lin_reg_data.csv', 'Population', 'Profit', pd.Series(np.zeros(2)))

gradientDescent('uni_lin_reg_data.csv', 'Population', 'Profit', 0.01, 1500)
