
import numpy as np
import pandas as pd


def rnnData(data, time_steps, isLabel=False):
    """
    creates new data frame based on previous observation
      * example:
        l = [1, 2, 3, 4, 5]
        time_steps = 2
        -> labels == False [[1, 2], [2, 3], [3, 4]] #Data frame for input with 2 timesteps
        -> labels == True [3, 4, 5] # labels for predicting the next timestep
    """
    rnn_df = []
    for i in range(len(data) - time_steps):
        if isLabel:
            rnn_df.append(data.iloc[i + time_steps].as_matrix())
        else:
            data_ = data.iloc[i: i + time_steps].as_matrix()
            if (len(data_.shape) > 1):
                rnn_df.append(data_)
            else:
                rnn_df.append([j] for j in data_)
    return np.array(rnn_df, dtype=np.float32)


def splitData(data, val_size=0.1, test_size=0.1):
    ntest = int(round(len(data) * (1 - test_size)))
    nval = int(round(len(data.iloc[:ntest]) * (1 - val_size)))
    df_train, df_val, df_test = data.iloc[:nval], data.iloc[nval:ntest], data.iloc[ntest:]
    return df_train, df_val, df_test

def prepData(data, timeSteps, isLabel=False):
    df_train, df_val, df_test = splitData(data)
    return (rnnData(df_train, timeSteps, isLabel=isLabel),
            rnnData(df_val, timeSteps, isLabel=isLabel),
            rnnData(df_test, timeSteps, isLabel=isLabel))

def genData(fct, x, timeSteps):
    sin = 5*fct(x)
    cos = 10*np.cos(x)
    input = sin
    input = np.c_[input, cos]
    input = np.c_[input, input]
    # input = np.c_[input, input]
    # input = np.c_[input, input]
    input = addNoise(input)

    output = np.multiply(sin, cos)
    output = np.add(output, 10*cos)
    output = np.c_[output, 50*np.sin(cos)]

    dataInput = pd.DataFrame(input)
    dataOutput = pd.DataFrame(output)
    x_train, x_valid, x_test = prepData(dataInput, timeSteps, isLabel=False)
    y_train, y_valid, y_test = prepData(dataOutput, timeSteps, isLabel=True)

    resultX = dict(train=x_train, val = x_valid, test = x_test)
    resultY = dict(train=y_train, val = y_valid, test = y_test)
    return resultX, resultY

def addNoise(x):
    np.random.seed(1)
    dim = x.shape[1]
    sig = 10
    mean = np.zeros(dim)
    cov = np.eye(dim, dtype=np.float32) * sig
    noise = np.asmatrix(np.random.multivariate_normal(mean, cov, len(x)), dtype = np.float32)

    result = np.add(x, noise)
    return result