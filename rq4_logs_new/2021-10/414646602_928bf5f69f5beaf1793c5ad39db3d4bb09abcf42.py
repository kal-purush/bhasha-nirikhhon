import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
from numpy import array, hstack
import statsmodels.api as sm
import matplotlib.pyplot as plt


def find_frequency(sampled_data, sampling_freq):
    
    ft = np.fft.rfft(sampled_data)
    freqs = np.fft.rfftfreq(len(sampled_data), 1.0/sampling_freq)
    mags = abs(ft)
    #plt.plot(freqs, mags)
    inflection = np.diff(np.sign(np.diff(mags)))
    peaks = (inflection < 0).nonzero()[0] + 1
    top_peaks = peaks[mags[peaks].argsort()[-5:]]
    freq_tops = freqs[top_peaks]
    return freq_tops.min()

def get_flat_line_forecast(last_value, ahead):
    
    return np.ones(ahead) * last_value

def get_mean_line_forecast(input_, ahead):
    
    return np.ones(ahead) * np.mean(input_)

def repeat_period_forecast_fourier(input_, sampling_freq, ahead):
    
    f_x = find_frequency(input_, sampling_freq)
    nb_sample_per_period = round(sampling_freq/f_x)
    last_period_value = input_[-nb_sample_per_period:]
    if ahead <= len(last_period_value):
        #print(last_period_value[:ahead])
        return last_period_value[:ahead]
    else:
        nb_period = int(ahead/nb_sample_per_period)
        repeated_period = np.array(last_period_value, copy=True)
        for i in range(nb_period -1):
            repeated_period = np.concatenate((repeated_period, last_period_value), axis = None)
        extra = ahead % nb_sample_per_period
        if extra:
            repeated_period = np.concatenate((repeated_period, last_period_value[:extra]), axis = None)
        #print(repeated_period)
        return repeated_period

def get_acf_peak(input_, lag):

    acf = sm.tsa.acf(input_, nlags = lag) 
    inflection = np.diff(np.sign(np.diff(acf)))
    peaks = (inflection < 0).nonzero()[0] + 1
    acf_peaks = np.argsort(-1*acf[peaks])
    acf_peaks_sorted = acf[peaks][acf_peaks]
    if len(acf_peaks_sorted) > 0:
        return acf_peaks_sorted[0]
    else:
        return 0
# reconstitute a batch of sample into a sequence, find its acf peak per axe, and compute the mean
def get_mean_acf_peak_value_for_signals(samples, lag, delay):
    
    reconstitued_signal_x = np.zeros((lag + (len(samples)-1)*delay))
    reconstitued_signal_y = np.zeros((lag + (len(samples)-1)*delay))
    reconstitued_signal_z = np.zeros((lag + (len(samples)-1)*delay))

    for i,sample in enumerate(samples):
        if i != len(samples)-1:
            reconstitued_signal_x[(i*delay): (i*delay) + delay] = sample[:,0][:delay]
            reconstitued_signal_y[(i*delay): (i*delay) + delay] = sample[:,1][:delay]
            reconstitued_signal_z[(i*delay): (i*delay) + delay] = sample[:,2][:delay]
        else: 
            reconstitued_signal_x[(i*delay):] = sample[:,0]
            reconstitued_signal_y[(i*delay):] = sample[:,1]
            reconstitued_signal_z[(i*delay):] = sample[:,2]

    #print(reconstitued_signal_x.shape)

    x_peak = get_acf_peak(reconstitued_signal_x, lag)
    y_peak = get_acf_peak(reconstitued_signal_y, lag)
    z_peak = get_acf_peak(reconstitued_signal_z, lag)

    return np.mean([x_peak,y_peak, z_peak])
    
def  repeat_period_forecast_acf(input_, lag, ahead, i):
    
    acf = sm.tsa.acf(input_, nlags = lag) 
    inflection = np.diff(np.sign(np.diff(acf)))
    peaks = (inflection < 0).nonzero()[0] + 1
    acf_top_peak = np.argsort(-1*acf[peaks])
    if len(acf_top_peak) < 1:
        return np.ones(ahead) * input_[-1:]
    top_peak_index = peaks[acf_top_peak][0]
    
    last_period_value = input_[-top_peak_index:]
    if ahead <= top_peak_index:
        #print(last_period_value[:ahead])
        return last_period_value[:ahead]
    else:
        nb_period = int(ahead/top_peak_index)
        repeated_period = np.array(last_period_value, copy=True)
        for i in range(nb_period -1):
            repeated_period = np.concatenate((repeated_period, last_period_value), axis = None)
        extra = ahead % top_peak_index
        if extra:
            repeated_period = np.concatenate((repeated_period, last_period_value[:extra]), axis = None)
        #print(repeated_period)
        return repeated_period
    

def fourierExtrapolation_dynamic(input_x, lag, ahead, sampling_freq):
    
    low_f = find_frequency(input_x, sampling_freq)
    num_sample_per_period = sampling_freq / low_f
    #print(num_sample_per_period)
    num_period = round(lag/ num_sample_per_period)
    #print(num_period)
    num_sample_for_ext = round(num_period * num_sample_per_period)
    offset = lag - num_sample_for_ext
    if offset!= 0:
        print(offset)
    new_input = input_x[offset:]

    n = new_input.size
    n_harm = 10                  # number of harmonics in model
    t = np.arange(0, n)
    #p = np.polyfit(t, input_x, 1)         # find linear trend in x
    #x_notrend = input_x - p[0] * t        # detrended x
    #x_freqdom = fft.fft(x_notrend)
    # detrended x in frequency domain
    x_freqdom = fft.fft(new_input) 
    
    f = fft.fftfreq(n) 
    indexes = list(range(n))
    indexes.sort(key=lambda i: np.absolute(x_freqdom[i]))
    indexes.reverse()
    #indexes = list(range(n)) 
    phase_df = ahead/n
    t = np.arange(0, n + ahead)
    restored_sig = np.zeros(t.size)
    for i in indexes[:1 + n_harm * 2]:
        ampli = np.absolute(x_freqdom[i]) / n  # amplitude
        phase = np.angle(x_freqdom[i])
        restored_sig += ampli * np.cos(2 * np.pi * f[i] * t + phase)
    return restored_sig 

# compute the baseline dictionary
# arguments are the X and y dataset, the global model if not none and present in the compariason methods
# list of possbile baseline (period acf, mean, period fourier, global model)
def get_forecast_baseline_dict(X_values, y_values, global_model, channels, lag, ahead, downsampling_freq, comparison_methods):

    dict_results = dict()

    for m in comparison_methods:
        dict_results[m] = []

    for i in range(channels):

        for comparison_method in comparison_methods:
            if comparison_method == "period acf":
                forecast_period_acf_channel = [repeat_period_forecast_acf(X_values[j][:, i], lag, ahead, j) for j in range(X_values.shape[0])]
                forecast_period_acf_channel = np.array(forecast_period_acf_channel)
                dict_results["period acf"].append(forecast_period_acf_channel)
            elif comparison_method == "mean":
                forecast_mean_channel = [get_mean_line_forecast(X_values[j][:, i], ahead) for j in range(X_values.shape[0])]
                forecast_mean_channel = np.array(forecast_mean_channel)
                dict_results["mean"].append(forecast_mean_channel)
            elif comparison_method == "period fourier":
                forecast_period_fourier = [repeat_period_forecast_fourier(X_values[j][:, i], downsampling_freq, ahead) for j in range(X_values.shape[0])]
                forecast_period_fourier = np.array(forecast_period_fourier)
                dict_results["period fourier"].append(forecast_period_fourier)

    if "global model" in comparison_methods:
            global_forecast = global_model.predict(X_values)
            for i in range(channels):
                global_forecast_channel = global_forecast[:,:,i].reshape((global_forecast.shape[0], global_forecast.shape[1]))
                dict_results["global model"].append(global_forecast_channel)

    return dict_results

import random

# plot a few forecast for a multi output model
def plot_forecasts_multi(X_true, y_true_list, list_forecasts_dict, dict_forecasts_color, k):
    
    random_indexes = random.sample(range(X_true.shape[0]), k)

    for i in random_indexes:
        
        fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(18,4))
        
        x_input = X_true[i][-200:,0].reshape((200,-1))
        y_input = X_true[i][-200:,1].reshape((200,-1))
        z_input = X_true[i][-200:,2].reshape((200,-1))

        x_true = y_true_list[0][i]
        y_true = y_true_list[1][i]
        z_true = y_true_list[2][i]

        t = [200 + j for j in range(len(x_true))]
        #print(t)

        inputs = [x_input, y_input,z_input]
        outputs = [x_true , y_true , z_true]

        signal_labels = ['x gyro', 'y gyro','z gyro']
        signal_colors = ['green','green','green']

        for j, ax in enumerate(axs.flatten()):
            ax.plot(range(len(inputs[j])), inputs[j], label = signal_labels[j], color = signal_colors[j])
            ax.plot(t, outputs[j], color = signal_colors[j])
            forecast_dict = list_forecasts_dict[j]
            for k,v in forecast_dict.items():
                ax.plot(t, v[i], label = k, color = dict_forecasts_color[k])

            ax.legend(loc="upper left")
            ax.set_xlabel("timesteps")
        plt.show()

def plot_forecasts_single(X_true, true, single_index, list_forecasts_dict, dict_forecasts_color, k):
    
    random_indexes = random.sample(range(X_true.shape[0]), k)

    for i in random_indexes:
        
        plt.figure()
        
        input_axis = X_true[i][-200:,single_index].reshape((200,-1))
        t = [200 + j for j in range(len(true))]

        signal_labels = ['input gyro']
        signal_colors = ['green','green','green']

        plt.plot(range(len(input_axis)), input_axis, label = "true gyro", color = "green")
        plt.plot(t, true, color = "green")
        forecast_dict = list_forecasts_dict[0]
        for k,v in forecast_dict.items():
            plt.plot(t, v[i], label = k, color = dict_forecasts_color[k])

        plt.legend(loc="upper left")
        plt.xlabel("timesteps")
        plt.show()
