from time import sleep
import sys
import os

import numpy as np
import matplotlib.pyplot as plt

sys.path.append('./cnn')
sys.path.append('./dataset')
import cnn_analysistool as ctool
import convolution_layer as cl
import logic_circuit as lc

(trainData, trainLabel) = lc.dset("mnist_train", 60000)
(testData, testLabel) = lc.dset("mnist_test", 10000)

def zero_padding(pad, train_data):
    (channel, height, width) = np.shape(train_data)
    # <zero padding>
    p_result = np.zeros([channel, height + 2*pad, width + 2*pad])
    for i in range(channel):
        p_result[i][pad:height + pad, pad:width + pad] = train_data[i]
    return p_result

def convert2mnist16(data):
    newdata = np.zeros([data.shape[0], 16, 16])
    for i in range(data.shape[0]):
        for j in range(0, 16):
            for k in range(0, 16):
                if (mode == "mean"):
                    newdata[i][j][k] = np.mean(data[i, 2 * j:2*j + 2, 2 * k:2*k + 2])
                elif (mode == "direct"):
                    newdata[i][j][k] = data[i][2 * j][2 * k]
        if (i % 100 == 0):
            sys.stdout.write('\r')
            # the exact output you're looking for:
            sys.stdout.write("[%-40s] %d%%" % ('*' * int(i / (data.shape[0] - 1) * 40) + "🎄",
                                               (i / data.shape[0] * 100) + 1))
            sys.stdout.flush()
    print("")
    return newdata

def save():
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    if (mode == "mean"):
        path = os.path.join(os.path.dirname(__file__), '../dataset/input/mnist16_mean')
    elif (mode == "direct"):
        path = os.path.join(os.path.dirname(__file__), '../dataset/input/mnist16_direct')
    np.savez(
        path,
        x_train=trainData,
        y_train=trainLabel,
        x_test=trainData,
        y_test=trainLabel,
    )
    np.warnings.filterwarnings('default', category=np.VisibleDeprecationWarning)

args = sys.argv
if len(args) != 2 or args[1] != "mean" or args[1] != "direct":
    sys.stdout.write("Error : useage  \"python3 tools/image_resize.py [direct or mean]\"\n")
    sys.exit(5)
trainData = zero_padding(2, trainData)
testData = zero_padding(2, testData)

mode = args[1]
print("\nConverting Train data...")
trainData = convert2mnist16(trainData)
print("\nConverting Tests data...")
testData = convert2mnist16(testData)
save()
print("\nSuccess 🎉")