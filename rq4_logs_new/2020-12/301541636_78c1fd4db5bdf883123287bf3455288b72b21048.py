# %% [markdown]
'''
# How to install NVIDIA GPU driver and CUDA on ubuntu
'''
# %% [code]
import os
import numpy as np
import matplotlib.pyplot as plt
import scipy.ndimage
import tensorflow as tf

# %% [code]
def read_data(filepath):
    price = []
    time = []
    
    with open(filepath) as f:
        lines = f.readlines()[1:]

    for data in lines:
        curr_time = float(data.split(",")[0])
        curr_price = float(data.split(",")[1][:-1])
        time += [curr_time]
        price += [curr_price]
        
    return np.array(price), np.array(time)

# %% [code]
filepath = "data/price_usd_close"
figure_dir = "reports/figures"

# loading the data
price, time = read_data(filepath)
time_shifted = time - time[0]

# %% [code]
# plotting the data.
# We can see that the price follows a exponential increase, because the following plot is roughly linear.
plt.figure()
plt.plot(time_shifted, price)
plt.title("Bitcoin price over time (USD)")
plt.savefig(os.path.join(figure_dir, "price.png"))
plt.show()
plt.close()

# %% [markdown]
'''
Two important properties : seasonality and stationarity
We will first analyse the seasonnality of the data, this will give us information such as at ahat frequency (in days) does the bitcoin price change, and also does it start.
After we will focus on the stationarity, does the local distribution of the price stays the same in a given period.
'''
# %% [markdown]
'''
# To analyse the seasonality of the bitcoin, we can make a fourier analysis to extract the most proeminent frequencies.
# Amplitude of the fft, or the frequencies values: describes at what frequency, in the data.
# Phase of the fft, or how the phase values: interresting to understand the dynamic of the price, when it starts
# if the output have a random white noise trend, then there is no evidence of principal frequence component(s).
'''
# %% code
#Before using the fft, we need to remove the non-stationnary component of the price.
#To do that we can simply take the derivative of the data
price_dt = price[1:] - price[:-1]
# if we want to keep the data length, we simply filter the data and remove this component to the original data
filt_price = scipy.ndimage.gaussian_filter1d(price, sigma=10)
price_centered = price - filt_price

# so now you can see that the resulting stationnaried prices are zero-centered
fig, axes = plt.subplots(2, figsize=(12, 8))
axes[0].plot(time_shifted, price)
axes[0].plot(time_shifted[:-1], price_dt)
axes[0].set_title('derivative')
axes[1].plot(time_shifted, price)
axes[1].plot(time_shifted, price_centered)
axes[1].set_title('substraction')
plt.savefig(os.path.join(figure_dir, "price_stationnarity.png"))
plt.show()
plt.close()

# %% code
# now we can properly compute the fft
price_fouried = tf.signal.fft(price_centered)

# %% code
# let's look at the magnitude and phase components
frequencies = np.arange(price_fouried.shape[0])
fig, axes = plt.subplots(2, figsize=(12, 8))
axes[0].plot(frequencies, tf.abs(price_fouried))
axes[0].set_title('fft magnitude')
axes[1].plot(frequencies, tf.math.angle(price_fouried))
axes[1].set_title('fft phase')
plt.savefig(os.path.join(figure_dir, "fft.png"))
plt.show()
plt.close()

# %% [markdown]
'''
To finish on seasonality anaylisis, let's take a look at the spectrogram of the data (derived from a time-frequency analysis).
This will give us lot of informations on the content at each time.
The spectrogram can be extracted using a short-fourier transform, which basically runs a fourier transform on a short window, sliding thourgh all the data.
'''

# %% [code]
# tensorflow provide a fast implementtaion of fast fourier transform.
# Here, we will have a window size of 100 samples (days), with a step of 1 and 250 frequency components
stft = tf.signal.stft(price_dt, frame_length=100, frame_step=1, fft_length=500, pad_end=True)
print(stft.shape)
spectrogram = tf.abs(stft).numpy()

# %% [code]
# Convert to log scale and transpose so that the time is represented in the x-axis (columns).
fig, axes = plt.subplots(2, figsize=(12, 8))
max_time = np.max(time_shifted)
axes[0].plot(time_shifted, price_centered)
axes[0].set_title('Waveform')
axes[0].set_xlim([0, max_time])
log_spec = np.log(spectrogram.T)
axes[1].pcolormesh(time_shifted, np.arange(log_spec.shape[0]), log_spec)
axes[1].set_xlim([0, max_time])
plt.savefig(os.path.join(figure_dir, "spectrogram.png"))
plt.show()

# %% [code]
# now we wil analyse the stationarity of the price
# if the data is stationarity, then a model can more easily predict its value (because of lessexternal perturbation)
#units in days
win_size = 15 #distribution of the data is calculated within 15 days
slide = 60 #we slide up to -/+ 60 days
corr = []

def data_distribution(inp):
    return np.histogram(inp, range=(0, 20000), bins=500, density=True)[0]

# loop through al timestamps
for i in np.arange(slide + int(win_size/2), len(price) - slide - int(win_size/2)):
    idx = i-int(win_size/2)
    # distribution of the price (over price from day -7.5 to day +7.5), the fixed distributioin
    fixed_price = price[idx:int(idx + win_size)]
    fixed_distrib = data_distribution(fixed_price)
    curr_corr = []
     # compare to each distribution at different timestamps (sliding from -30 to +30), the moving distribution 
    for offset in np.arange(-slide, slide + 1):
        idx = offset + i - int(win_size/2)
        moving_price = price[idx:(idx + win_size)]
        moving_distrib = data_distribution(moving_price)
        curr_corr += [np.correlate(fixed_distrib, moving_distrib)]
    curr_corr = curr_corr / np.max(curr_corr)    
    corr += [curr_corr]    
    if i%200 == 0:
        print("day {}".format(i))

output = np.array(corr)[:, :, 0]

# %% [code]
plt.imshow(output, cmap="gray")
plt.axis("tight")
plt.show()
plt.imsave(os.path.join(figure_dir, "range_accuracy.png"), corr, cmap="gray")