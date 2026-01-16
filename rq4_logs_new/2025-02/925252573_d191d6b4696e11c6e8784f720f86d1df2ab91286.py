import numpy as np
import matplotlib.pyplot as plt

# Define the sequence
x = np.array([1, -1, 2, -2, 1, 2])
N_x = len(x)

# Perform the DFT using the FFT algorithm (Decimation-in-Time)
X = np.fft.fft(x, N_x)

# Compute frequencies for plotting
frequencies = np.fft.fftfreq(N_x)

# Plot the magnitude spectrum
plt.figure(figsize=(10, 6))
plt.stem(frequencies, np.abs(X), basefmt=" ", use_line_collection=True)
plt.title("Decimation-in-Time DFT Magnitude Spectrum")
plt.xlabel("Normalized Frequency (cycles/sample)")
plt.ylabel("Magnitude")
plt.grid(True)
plt.show()