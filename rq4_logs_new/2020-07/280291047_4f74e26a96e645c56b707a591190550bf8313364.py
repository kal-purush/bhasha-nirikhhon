import wave
import math
from numpy import *
from pylab import *
from struct import *


def printWaveInfo(wf):
    """WAVEファイルの情報を取得"""
    print("チャンネル数:", wf.getnchannels())
    print ("サンプル幅:", wf.getsampwidth())
    print ("サンプリング周波数:", wf.getframerate())
    print ("フレーム数:", wf.getnframes())
    print ("パラメータ:", wf.getparams())
    print ("長さ（秒）:", float(wf.getnframes()) / wf.getframerate())


# 振幅スペクトルを考えるため絶対値をとる。最大振幅が１に正規化
def regularization(np_array):
    np_array = np.abs(np_array)
    max = np.max(np_array)
    print(max)
    np_array = np_array/np.max(np_array)
    return np_array

# Anを計算 
def rep_an_data(np_array, data, num):
    for n in range(num):
        area = 0
        for ft,t in enumerate(data):
            area += ft * math.cos(2*math.pi*n*t/T0)
        np_array = np.append(np_array, area*2/T0)
    return np_array

# Bnを計算
def rep_bn_data(np_array, data, num):
    for n in range(num):
        area = 0
        for ft,t in enumerate(data):
            area += ft * math.sin(2*math.pi*n*t/T0)
        np_array = np.append(np_array, area*2/T0)
    return np_array

if __name__ == '__main__':
    wf = wave.open("r3.wav", "rb")
    buffer = wf.readframes(wf.getnframes())
    # printWaveInfo(wf)

    N= 512
    fs = 44100
    T0 = N/fs

    han = np.hanning(N)    # ハニング窓

     # bufferはバイナリなので2バイトずつ整数（-32768から32767）にまとめる
    originaldata = frombuffer(buffer, dtype="int16")
    datafloat = originaldata[:N] * han
    dataint = np.array(datafloat, dtype=int)
    # print(dataint)
    # plot(datafloat[:N])
    # plot(originaldata[:N])
    # show()

    an_array = np.array([])
    a0 = dataint.sum()/T0

    an_array = np.append(an_array,a0)
    an_array = rep_an_data(an_array,dataint, int(N/2))
    an_array = regularization(an_array)
    # print(an_array)
    # plot(an_array)
    # show()
    
    bn_array = np.array([])
    bn_array = rep_bn_data(bn_array,dataint, int(N/2))
    bn_array = regularization(bn_array)
    print(bn_array)
    plot(bn_array)
    show()