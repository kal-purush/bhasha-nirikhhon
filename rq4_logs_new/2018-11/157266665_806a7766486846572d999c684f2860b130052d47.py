import os
import sys
import random
import warnings

import numpy as np
import pandas as pd
import time

import matplotlib.pyplot as plt
import io
import pickle
import json

from tqdm import tqdm
from itertools import chain
from skimage.io import imread, imshow, imread_collection, concatenate_images
from skimage.transform import resize
from skimage.morphology import label

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torch.utils.data import sampler

#import torchvision.datasets as dset
#import torchvision.transforms as T
import torch.nn.functional as F
from basic import *

# Constant to control how frequently we print train loss
print_every = 1

def trainModel(model, x_train, y_train, optimizer, epochs = 1, mini_batch_size = 64, noVal = False):
    #model = model.to(device=device)  # move the model parameters to CPU/GPU
    T = 0
    num_batches = int(len(x_train)/mini_batch_size)
    num_remaining = len(x_train) - num_batches * mini_batch_size
    loss_history = []
    # loss = nn.CrossEntropyLoss()
    for e in range(epochs):
        for t in range(num_batches):
            rand_indices = np.random.choice(len(x_train), mini_batch_size)
            x = torch.from_numpy(x_train[rand_indices, :, :, :])
            y = torch.from_numpy(y_train[rand_indices])
            model.train()  # put model to training mode
            #x = x.to(device=device, dtype=dtype)  # move to device, e.g. GPU
            #y = y.to(device=device, dtype=dtype)
            y = y.type(torch.LongTensor)
            print('y shape: ', y.shape)

            preds = model(x)
            _, predicted = torch.max(preds.data, 1)
            correct += (predicted == y).sum().item()

            loss = F.cross_entropy(preds, y)

            # Zero out all of the gradients for the variables which the optimizer
            # will update.
            optimizer.zero_grad()

            # This is the backwards pass: compute the gradient of the loss with
            # respect to each  parameter of the model.
            loss.backward()

            # Actually update the parameters of the model using the gradients
            # computed by the backwards pass.
            optimizer.step()

            if T % print_every == 0:
                currLoss = loss.item()
                loss_history.append(currLoss)
                print('Epoch %d, Iteration %d, loss = %.4f' % (e, t, currLoss))
            if (num_remaining <= 0 and t == (num_batches -1)):
                # perf = calculatePerformance(x_train, y_train, model)
                perf = (correct/(float(len(x_train))))
                print('Train performance at epoch %d is %.4f' % (e, perf))
                if (noVal == False):
                    # perf = calculatePerformance(X_val, Y_val, model)
                    print('Val performance at epoch %d is %.4f' % (e, perf))
            T +=1
        if num_remaining > 0:
            rand_indices = np.random.choice(len(x_train), num_remaining)

            x = torch.from_numpy(x_train[rand_indices, :, :, :])
            y = torch.from_numpy(y_train[rand_indices])
            model.train()  # put model to training mode
            #x = x.to(device=device, dtype=dtype)  # move to device, e.g. GPU
            #y = y.to(device=device, dtype=dtype)
            y = y.type(torch.LongTensor)



            preds = model(x)
            _, predicted = torch.max(preds.data, 1)


            # issue was with an incorrect performance function (i think)
            correct = 0
            for i in range(len(predicted)):
                if predicted[i] == y[i]:
                    correct += 1
            # correct += (predicted == y).sum().item()

            # print(predicted)
            # print(y)
            #values, indices = torch.max(preds, 1)
            loss = F.cross_entropy(preds, y)
            #loss(preds, y)

            # Zero out all of the gradients for the variables which the optimizer
            # will update.
            optimizer.zero_grad()

            # This is the backwards pass: compute the gradient of the loss with
            # respect to each  parameter of the model.
            loss.backward()

            # Actually update the parameters of the model using the gradients
            # computed by the backwards pass.
            optimizer.step()
            if T % print_every == 0:
                currLoss = loss.item()
                loss_history.append(currLoss)
                print('Epoch %d, Iteration %d, loss = %.4f' % (e, num_batches, currLoss))
            perf = (correct/(float(len(x))))
            print('Train performance at epoch %d is %.4f' % (e, perf))
            if (noVal == False):
                # perf = (correct/(float(len(x_train))))

                print('Val performance at epoch %d is %.4f' % (e, perf))
            T +=1
    return perf, loss_history

def showVisualComparisons(X, y, ex):
    plt.imshow(np.uint8(np.reshape(X[ex, :], (28, 28))))
    plt.show()
    print(Y_train[:, ex])

def main():
    # For this cell used same code from PyTorch notebook in assignment 2 of Stanford's CS231n Spring 2018 offering
    preprocessData = False # To preprocess data set this to True
    USE_GPU = False
    dtype = torch.float32
    if USE_GPU and torch.cuda.is_available():
        device = torch.device('cuda')
    else:
        device = torch.device('cpu')
        dtype = torch.float32

    print('using device:', device)

    # Next two cells, code belongs to [1]. Minor changes made to accomodate to our use
    # (Using PyTorch instead of Keras/tensorflow)
    IMG_WIDTH = 28
    IMG_HEIGHT = 28
    IMG_CHANNELS = 1
    PATH = './'
    epsilon = 1e-12 #For numerical stability

    warnings.filterwarnings('ignore', category=UserWarning, module='skimage')
    seed = 1
    random.seed = seed
    np.random.seed = seed


    # Preproccessing data
    trainCSV = "npy_data/train_npy.csv"
    trainDF = pd.read_csv(trainCSV, header = 0)
    trainDF = trainDF.values

    X_t = trainDF[:, 0:-5]
    X_train = np.zeros((len(X_t), 1, 28, 28), dtype= np.float32)
    for i, row in enumerate(X_t):
        X_train[i] = np.reshape(X_t[i, :], (1, 28, 28))
        # want shape [samples, 1, 28, 28]

    Y_t = trainDF[:, -5:]   #label x sample
    Y_train = np.zeros((Y_t.shape[0]))
    # Pytorch needs indices
    for i, row in enumerate(Y_t):
        Y_train[i] = np.argmax(row)


    devCSV = "npy_data/dev_npy.csv"
    devDF = pd.read_csv(devCSV, header = 0)
    devDF = devDF.values
    X_d = devDF[:, 0:-5]
    X_dev = np.zeros((len(X_d), 1, 28, 28), dtype= np.float32)
    for i, row in enumerate(X_d):
        X_dev[i] = np.reshape(X_d[i, :], (1, 28, 28))
    Y_d = devDF[:, -5:]   #label x sample
    Y_dev = np.zeros((Y_d.shape[0]))
    for i, row in enumerate(Y_d):
        Y_train[i] = np.argmax(row)
    print ("X_train shape: " + str(X_train.shape))
    print ("Y_train shape: " + str(Y_train.shape))
    print ("X_dev shape: " + str(X_dev.shape))
    print ("Y_dev shape: " + str(Y_dev.shape))

    # train_file = 'npy_data/train_npy.csv'
    # train = pd.read_csv(train_file)
    #
    # test_file = 'npy_data/test_npy.csv'
    # test = pd.read_csv(test_file)
    #
    # dev_file = 'npy_data/dev_npy.csv'
    # dev = pd.read_csv(dev_file)

    # Overfitting data first
    bestPerf = -1
    lossHistory = None
    lossHistories = {}
    print_every = 1
    bestModel = None
    bestLoss = 10000
    lrUsed = 0
    x_train = X_train[0:50, :, :, :]
    y_train = Y_train[0:50]
    lrs = []
    for i in range(4):
        lrs.append(5*np.random.rand()*1e-3)
    # lrs = [1e-7,1e-6,1e-5,1e-4,1e-3]
    lrs.append(.002147418314081924) # Best result from last random searches
    for lr in lrs:
        print('Trying out learning rate of ', lr)
        model = NNet()
        optimizer = optim.Adam(model.parameters(), lr = lr)
        modelPerf = trainModel(model, x_train, y_train, optimizer, epochs = 25, noVal = True)
        lossHistories[str(lr)] = modelPerf[1]
        if modelPerf[1][len(modelPerf[1])-1] < bestLoss:
            bestLoss = modelPerf[1][len(modelPerf[1])-1]
            bestPerf = modelPerf[0]
            lossHistory = modelPerf[1]
            bestModel = model
            lrUsed = lr

if __name__ == '__main__':
	main()