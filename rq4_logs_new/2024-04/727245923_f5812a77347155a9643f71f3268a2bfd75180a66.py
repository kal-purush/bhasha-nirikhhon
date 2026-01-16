from datetime import datetime

print("____  _____")
print("Olie\\/ortex Analytics: ProductionLi.py")
print()
print(f"Create Lifted Index predictions from Deep Learning Model")
print()
print(f"Effective date: 2010 - 2023 Chanhassen")

start = datetime.now()
import matplotlib.pyplot as plt
import pandas as pd
import pickle
import torch

from sklearn.feature_selection import SelectKBest
from sklearn.preprocessing import StandardScaler

print(f"Imports        : {(datetime.now()-start).total_seconds()*1000.:.2f}ms")
start = datetime.now()

D = 50
K = 1

def load_dataset():
    df = pd.read_csv('c:/workspace/dillon.csv')
    df['li'] = pd.read_csv('c:/workspace/li.csv')

    # Remove data with NaN values
    df = df.dropna()

    # Separate the datasets
    X = df.drop(['date', 'hour', 'li'], axis=1)
    Y = df['li'].values
    
    # Scale the X dataset
    ss = load_standard_scaler() 
    X = ss.transform(X)

    # Select the best n features
    skb = load_k_best()
    X = skb.transform(X)
   
    return X, Y

def load_standard_scaler()-> StandardScaler:
    with open('c:/workspace/li_std_scaler.skl', 'rb') as f:
        std_scaler = pickle.load(f)

    return std_scaler

def load_k_best()-> SelectKBest:
    with open('c:/workspace/li_k_best.skl', 'rb') as f:
        k_best = pickle.load(f)

    return k_best

model = torch.nn.Sequential()
model.add_module("dense1", torch.nn.Linear(D, 7))
model.add_module("tanh1", torch.nn.Tanh())
model.add_module("dense2", torch.nn.Linear(7, K))
model.load_state_dict(torch.load('c:/workspace/li_fnn.pt'))
model.eval()

X, Y = load_dataset()

logits = model.forward(torch.from_numpy(X).float())
Y_pred = logits.data.numpy()

print(f"Make Prediction: {(datetime.now()-start).total_seconds()*1000.:.2f}ms")
start = datetime.now()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

ax1.title.set_text('Interesting Region')
ax1.scatter(Y, Y_pred)
ax1.plot((-14, 50), (-14, 50), c='k')
ax1.set_xlabel('actual')
ax1.set_ylabel('prediction')
ax1.set_ylim(-14, 6)
ax1.set_xlim(-14, 6)

ax2.title.set_text('Full Region')
ax2.scatter(Y, Y_pred)
ax2.plot((-14, 50), (-14, 50), c='k')
ax2.set_xlabel('actual')
ax2.set_ylabel('prediction')

print(f"Draw Graph     : {(datetime.now()-start).total_seconds()*1000.:.2f}ms")
start = datetime.now()

plt.show()