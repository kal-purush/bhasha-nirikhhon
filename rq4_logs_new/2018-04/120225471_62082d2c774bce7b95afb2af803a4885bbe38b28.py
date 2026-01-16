from utils.model import build_model
import h5py
import numpy as np
import matplotlib.pyplot as plt
import argparse
from scipy import signal
from features.simple_filter import SimpleSplineFilter, SimpleLocalNorm, normalize
from utils import thermistor
import os

def plot_max():
    figManager = plt.get_current_fig_manager()
    figManager.window.showMaximized()
    plt.show()

def visualize_stft_extractor(input_path, model_json):
    # Parse the input data
    data = h5py.File(input_path, 'r')['data']

    input_signal = data['signal']
    target_signal = data['target']
    sampling_rate = data.attrs['sampling_rate']
    ts = np.linspace(0, input_signal.size/sampling_rate, input_signal.size)

    # Reconstruct the model
    model = build_model(sampling_rate, model_json)
    model_out = model(input_signal)

    target_signal = normalize(target_signal)
    filtered_target = SimpleSplineFilter(sampling_rate, [], {'local_window_length':60/200,'ds':20,'s':45}).calc_feature(target_signal)
    filtered_target = SimpleLocalNorm(sampling_rate, [], {"local_window_length":40}).calc_feature(filtered_target)

    centroid = thermistor.stft_centroid_bpm(model_out, sampling_rate)
    tcentroid = thermistor.stft_centroid_bpm(target_signal, sampling_rate)

    fig, ax = plt.subplots(1,1)
    ax.plot(centroid, label="Predicted Centroid RR")
    ax.plot(tcentroid, label="Target Centroid RR")
    ax.set_xlabel("Time in s")
    ax.set_ylabel("RR in bpm")
    ax.set_title("STFT Centroid of {} and Thermistor on {}".format(model.name, os.path.basename(input_path).split('.')[-2]))
    plt.legend()
    plot_max()

    rmean_sq_error_centroid = np.sqrt(np.mean((tcentroid-centroid)**2))
    print("Root Mean Squared Error of Centroid: {}".format(rmean_sq_error_centroid))

    model_bpm = thermistor.instant_bpm(model_out)
    t_bpm = thermistor.instant_bpm(target_signal)

    fig, ax = plt.subplots(1,1)
    ax.plot(model_bpm, label="Predicted Instant RR")
    ax.plot(t_bpm, label="Target Instant RR")
    ax.set_xlabel("Time in s")
    ax.set_ylabel("RR in bpm")
    ax.set_title("Instant RR of {} and Thermistor on {}".format(model.name, os.path.basename(input_path).split('.')[-2]))
    plt.legend()
    plot_max()

    rmean_sq_error_bpm = np.sqrt(np.mean((t_bpm-model_bpm)**2))
    print("Root Mean Squared Error of Instant BPM: {}".format(rmean_sq_error_bpm))

if __name__ == "__main__":
    # Parse Arguments
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('data_path', type=str, help='Path to h5 file to be visualized.')
    parser.add_argument('model_json', type=str, help='Path to json of model to be used as the feature.')
    # parser.add_argument('thing', type=int, help='Name of the dataset under raw containing the data folder of h5 files to be processed.')
    args = parser.parse_args()

    model_json = args.model_json
    input_path = args.data_path

    visualize_stft_extractor(input_path, model_json)