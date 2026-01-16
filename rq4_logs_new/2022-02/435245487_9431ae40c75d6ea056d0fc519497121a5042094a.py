import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def split_sequence_from_dataframe(dataset, window_size):
    ds = pd.DataFrame(index=range(len(dataset)))
    for cols in dataset.columns.values:
        for i in range(window_size+1):
            ds[f"{cols}(t + {i})"] = dataset.shift(-i)

    ds.dropna(inplace=True)
    X = ds.iloc[:,:-1]
    y = ds.iloc[:,-1]
    return X, y


def PlotTimeSeriesValidation(dataset):
    N=np.max(dataset.index)
    fig, ax = plt.subplots(figsize=(18,8))
    dataset.plot(0,1,figsize=(18,8), ax=ax)
    ax.axvspan(dataset.index[0], dataset.index[int(N*0.8)], color='red' ,alpha=0.5)
    ax.axvspan(dataset.index[int(N*0.8)], dataset.index[N] ,alpha=0.5)
    plt.text(dataset.index[int(N*0.3)], 620, "80% Training set", fontsize=15)
    plt.text(dataset.index[int(N*0.85)], 620, "20% Test set", fontsize=15)