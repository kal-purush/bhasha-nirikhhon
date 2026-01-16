from utils.model import build_model
import h5py
import numpy as np
import matplotlib.pyplot as plt
import argparse
from scipy import signal
from features.simple_filter import SimpleSplineFilter, SimpleLocalNorm, normalize


def plot_max():
    figManager = plt.get_current_fig_manager()
    figManager.window.showMaximized()
    plt.show()

def visualize_model(input_path, model_json):
    # Parse the input data
    data = h5py.File(input_path, 'r')['data']

    input_signal = data['signal']
    target_signal = data['target']
    sampling_rate = data.attrs['sampling_rate']
    ts = np.linspace(0, input_signal.size/sampling_rate, input_signal.size)

    # Reconstruct the model
    model = build_model(sampling_rate, model_json)

    target_signal = normalize(target_signal)
    filtered_target = SimpleSplineFilter(sampling_rate, [], {'local_window_length':60/200,'ds':20,'s':45}).calc_feature(target_signal)
    filtered_target = SimpleLocalNorm(sampling_rate, [], {"local_window_length":40}).calc_feature(filtered_target)

    # Loop over all features and plot them against thermistor output and raw input
    print(model.get_names())
    print(model.get_list())
    for (name, feature) in zip(model.get_names(), model.get_list()):
        print(name)
        # visualize raw output
        feature_out = feature.calc_feature(input_signal)

        fig, ax = plt.subplots(1,1)
        ax.set_title("Feature: {}".format(name))
        ax.plot(ts, feature_out, label=name)
        ax.plot(ts, filtered_target, label="thermistor")
        # ax.plot(ts, normalize(input_signal), label="input")
        ax.set_xlabel("time in seconds")
        plt.legend()
        plt.show()
        # plot_max()
        print(name)

if __name__ == "__main__":
    # Parse Arguments
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('data_path', type=str, help='Path to h5 file to be visualized.')
    parser.add_argument('model_json', type=str, help='Path to json of model to be used as the feature.')
    # parser.add_argument('thing', type=int, help='Name of the dataset under raw containing the data folder of h5 files to be processed.')
    args = parser.parse_args()

    model_json = args.model_json
    input_path = args.data_path

    visualize_model(input_path, model_json)