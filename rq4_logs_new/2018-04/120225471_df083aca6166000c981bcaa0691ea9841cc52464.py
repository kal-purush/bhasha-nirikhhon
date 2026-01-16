from utils.model import build_model
import h5py
import numpy as np
import matplotlib.pyplot as plt
import argparse

def plot_max():
    figManager = plt.get_current_fig_manager()
    figManager.window.showMaximized()
    plt.show()

def visualize_stft_extractor(input_path, model_json):
    # Parse the input data
    data = h5py.File(dataset_path, 'r')['data']

    input_signal = data['signal']
    target_signal = data['target']
    sampling_rate = data.attrs['sampling_rate']
    ts = np.linspace(0, input_signal.size/sampling_rate, input_signal.size)

    # Reconstruct the model
    model = build_model(sampling_rate, model_json)

    # Visualize raw output
    model_out = model(input_signal)
    fig, ax = plt.subplots(2,1)
    ax[0].set_title("Model Output")
    ax[0].plot(ts, model_out, label="Raw Input Signal")

    ax[1].set_title("Raw Target Signal")
    ax[1].plot(ts, target_signal, label="Raw Target Signal")

    ax[0].set_xlabel("Time in seconds")
    ax[1].set_xlabel("Time in seconds")
    plot_max()

    fig, ax = plt.subplots(2,1)
    ax[0].set_title("Model Output")
    ax[0].plot(ts, model_out, label="Raw Input Signal")

    ax[1].set_title("Raw Target Signal")
    ax[1].plot(ts, target_signal, label="Raw Target Signal")

    ax[0].set_xlabel("Time in seconds")
    ax[1].set_xlabel("Time in seconds")
    plot_max()

    # Visualize STFT
    fig, ax = plt.subplots(1,1)
    max_freq = 40/60
    downsample = 4
    f,t, Zxx = signal.stft(model_out[::downsample], fs=sampling_rate/downsample, nperseg=8000//downsample, noverlap=8000//downsample-10, boundary=None)
    max_bin = np.searchsorted(f, max_freq)
    ax.set_xlabel("Time in s")
    ax.set_ylabel("RR in bpm")
    ax.pcolormesh(t, f[2:max_bin]*60, np.sqrt(np.abs(Zxx)[2:max_bin]))
    # ax2.set_ylim(f[2]*60, f[max_bin]*60) #f[max_bin]*60)
    plot_max()

if __name__ == "__main__":
    # Parse Arguments
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('data_path', type=str, help='Path to h5 file to be visualized.')
    parser.add_argument('model_json', type=str, help='Path to json of model to be used as the feature.')
    # parser.add_argument('thing', type=int, help='Name of the dataset under raw containing the data folder of h5 files to be processed.')
    args = parser.parse_args()

    model_json = args.model_json
    input_path = args.data_path

    visualize_dataset(input_path, model_json)