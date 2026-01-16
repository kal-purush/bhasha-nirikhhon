import numpy as np
from scipy.signal import argrelextrema
from GetPara import get_para
import matplotlib.pyplot as plt

def smooth_data(data, window_size):
    return data.rolling(window=window_size, center=True).mean()

def find_peaks_troughs(data, order):
    # 局所的な最大値（ピーク）を検出
    peaks = argrelextrema(data.values, np.greater, order=order)[0]
    # 局所的な最小値（谷）を検出
    troughs = argrelextrema(data.values, np.less, order=order)[0]
    return peaks, troughs

def bry_boschan(data, window_size, order):
    # データをスムージング
    smoothed_data = smooth_data(data, window_size)
    
    # ピークと谷を検出
    peaks, troughs = find_peaks_troughs(smoothed_data.dropna(), order)

    d = [i*-1 for i in troughs]
    e = [i*-1 for i in troughs]
    for i in range(len(peaks)):
        d.append(peaks[i])
        e.append(peaks[i])
    d.sort(key=abs)

    for i in range(10000):
        if (i == len(d)-1):
            break
        if (d[i] < 0 and d[i+1] < 0):
            max_price = max(smoothed_data[abs(d[i]):abs(d[i+1])])
            for j in range(abs(d[i]), abs(d[i+1])):
                if (smoothed_data[j] == max_price):
                    e.append(j)
            
        if (d[i] > 0 and d[i+1] > 0):
            min_price = min(smoothed_data[d[i]:d[i+1]])
            for j in range(d[i], d[i+1]):
                if (smoothed_data[j] == min_price):
                    e.append(j*-1)

    e.sort(key=abs)

    peaks = [x for x in e if x > 0]
    troughs = [abs(x) for x in e if x < 0]

    return peaks, troughs

df = get_para('N225', 20040101, 20240101)

# Bry-Boschan法を適用
peaks, troughs = bry_boschan(df['price'], 10, 30)

for i in range(min(len(peaks), len(troughs)-1)):
    if(peaks[0] < troughs[0]):
        plt.axvspan(df.index[peaks[i]], df.index[troughs[i]], color='gray', alpha=0.3)
    else:
        plt.axvspan(df.index[peaks[i]], df.index[troughs[i+1]], color='gray', alpha=0.3)


# ピークと谷の位置に印をつけてプロット
plt.plot(df.index, df['price'], color='black', label='stock prices', linestyle='solid', alpha=1.0, linewidth=0.5)
plt.title('N225')
# plt.plot(df['price'].index[peaks], df['price'].iloc[peaks], 'ro', label='Peaks')
# plt.plot(df['price'].index[troughs], df['price'].iloc[troughs], 'go', label='Troughs')
plt.show()