# -*- encoding: utf-8 -*-
'''
@File    :   Linear_Regression_Model.py
@Author  :   Yixing Lu
@Time    :   2021/04/12 14:47:10
@Software : VS Code
'''

import numpy as np
import random
import pandas as pd
import matplotlib.pyplot as plt


def read(file_path):
    data = np.array(pd.read_csv(file_path,header=None))
    num_features = data.shape[-1]-1
    training_examples = data.shape[0]
    # shuffle the data set
    # Method 1
    index = [i for i in range(len(data))]
    random.shuffle(index)
    data = data[index]
    input_X = data[...,:num_features].reshape(num_features,training_examples)
    # shape: (n,m)
    output_Y = data[...,-1].reshape(1,training_examples)
    # shape: (1,m)
    return [input_X,output_Y]

# shuffle data set
# Method 2
def shuffle_data(x,y):
    # x.T shape = (m,n)
    # y.T shape = (m,1)    
    data = np.array([x.T,y.T])
    # data.shape = (2,m,)
    data = data.transpose(1,0,2)
    # data.shape = (m,2,)
    # shuffle the m training examples
    np.random.shuffle(data)
    # extract input_x and output_y: x.shape = (m,n) y.shape = (m,1)
    x = np.array(data[:,0,:]).transpose()
    y = np.array(data[:,1,:]).transpose()
    return [x,y]

# feature scaling
def normalization(x):
    avg = np.mean(x,axis = 1)
    std = np.std(x,axis = 1)
    x = (x-avg)/std
    return x


def Weight_initialization(input_X):
    return np.random.rand(1,input_X.shape[0])

def Bias_initialazation():
    return np.zeros((1,1))

def linear_forward(input_X,W,B):
    Y_hat = np.dot(W,input_X)+B
    return Y_hat

def compute_loss(output_Y,Y_hat):
    total_loss = 2*np.mean((Y_hat-output_Y)**2)
    # output_Y.shape[1] = number of training examples
    return total_loss

def compute_gradient(W,B,Y_hat,output_Y,input_X):
    W_gradient = np.dot(Y_hat - output_Y,input_X.T)/output_Y.shape[1]
    B_gradient = np.sum(Y_hat - output_Y,keepdims=True)/output_Y.shape[1]
    # Gradient Clipping (for solving gradient explosion)
    # You can use either this or AdamOptimizer (see below) to prevent gradient explosion
    # I choose the threshold as 0.5    
    # threshold_norm = 0.5
    # if np.linalg.norm(W_gradient)>threshold_norm:
    #     W_gradient = threshold_norm*W_gradient/np.linalg.norm(W_gradient)
    # if np.linalg.norm(B_gradient)>threshold_norm:
    #     B_gradient = threshold_norm*B_gradient/np.linalg.norm(B_gradient)    
    return [W_gradient,B_gradient]

# apply gradient descent (update parameters)
def apply_gradient(W,B,W_gradient,B_gradient,learning_rate):
    W_update = W - learning_rate * W_gradient
    B_update = B - learning_rate * B_gradient
    return [W_update,B_update]
    
    
if __name__ == "__main__":
    input_X, output_Y = read("data.csv")
    X = normalization(input_X)
    Y = output_Y
    training_examples = input_X.shape[1]
    num_features = input_X.shape[0]
    batch_size = 50
    start = 0
    W = Weight_initialization(input_X)
    B = Bias_initialazation()
    iteration = 1000
    
    # Method 1: gradient descent
    for i in range(iteration):
        # use Batch-training
        start = i * batch_size % input_X.shape[1]
        end = min(start+batch_size,output_Y.shape[1])
        X_train = X[:,start:end]
        Y_train = Y[:,start:end]
        # forward-prop
        Y_hat = linear_forward(X_train,W,B)
        # compute loss
        if i % 100 == 0:
            loss = compute_loss(Y_train,Y_hat)
            print("After %d iterations, the error is %g" %(i,loss))
        
        # back-prop with Adam Optimizer
        # compute original gradient        
        W_gradient,B_gradient = compute_gradient(W,B,Y_hat,Y_train,X_train)
        
        # # Momentum and RMSprop
        # if i == 0:
        #     v_w,v_b = 0.1*W_gradient,0.1*B_gradient
        #     s_w,s_b = 0.001*W_gradient**2,0.001*B_gradient**2
        # else:
        #     v_w = 0.9*v_w+0.1*W_gradient
        #     v_b = 0.9*v_b+0.1*B_gradient
        #     s_w = 0.999*s_w +0.001*W_gradient**2
        #     s_b = 0.999*s_b + 0.001*B_gradient**2

        # W_gradient = v_w/(np.sqrt(s_w)+pow(10,-8))
        # B_gradient = v_b/(np.sqrt(s_b)+pow(10,-8))

        # gradient descent
        W,B = apply_gradient(W,B,W_gradient,B_gradient,learning_rate = 0.1)    
    w = np.squeeze(W)
    b = np.squeeze(B)
    error_1 = np.mean((w*X+b-Y)**2)
    print("The result of Gradient Descent\n After %d iterations, Weight: %g Bias: %g Error: %g" %(iteration,w,b,error_1))
       
    # Method 2: Normal Equation
    # add a col for X to use intercept for normal equation
    b_add = np.ones((1,training_examples))
    X_train_2 = np.insert(input_X,num_features,b_add,axis = 0)
    X_train_2 = X_train_2.T
    # shape: (100,2)
    Y_train_2 = output_Y.T
    # shape: (100,1)
    theta = np.array(np.dot(np.dot(np.linalg.inv(np.dot(X_train_2.T,X_train_2)),X_train_2.T),Y_train_2))
    error_2 = np.mean((np.dot(theta.T,X_train_2.T)-Y_train_2.T)**2)
    theta_w = theta[0,0]
    theta_b = theta[1,0]
    print("The result of Normal Equation\n Weight: %g Bias: %g Error: %g"%(theta_w,theta_b,error_2))
    

    # Visualization
    x = input_X.flatten()
    y = output_Y.flatten()
    x_1 = X.flatten()
    y_1 = Y.flatten()
    fig = plt.figure()
    fig.suptitle("Linear Regression",fontsize = 11)
    plt.subplot(121)
    plt.scatter(x_1,y)
    plt.plot(x_1,w*x_1+b,c = 'r')
    plt.title("Linear Regression \nwith Gradient Descent",fontsize = 8)
    plt.subplot(122)
    plt.scatter(x,y)
    plt.plot(x,theta_w*x+theta_b,c = 'r')
    plt.title("Linear Regression \nwith Normal Equation",fontsize = 8)
    plt.savefig("Linear_Regression.png")
    plt.show()