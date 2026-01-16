# Syntax 
# $ python Vibration_Graphs <csv file path> <Sampling Frequency> <FFT Points> <HP cutt-off frequency in Khz> <LP cutt-off frequency in Khz> 
# 
# LP & HP filters form the bandgap filter used to extract the frequency activity around the machine reasonance frequency
#
# Example
# $ python Vibration_Graphs ./x.csv 12800 1024 2.5 5

from scipy.fft import rfft
from scipy.fftpack import rfftfreq
from scipy.stats import kurtosis
from scipy.signal import butter, lfilter, hilbert
import array
import pandas
import numpy
import matplotlib.pyplot
import sys


#Defining bandpass filter
def butter_bandpass(lowcut, highcut, fs, order=5):
    return butter(order, [lowcut, highcut], fs=fs, btype='band')

def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    b, a = butter_bandpass(lowcut, highcut, fs, order=order)
    y = lfilter(b, a, data)
    return y


file_name = sys.argv[1] #Argument 1 is the .csv (Column 0 includes acceleration in g)
Fs = int(sys.argv[2]) #Argument 2 is the Sampling Frequency in Hz
FFT_PT = int(sys.argv[3]) #Argument 3 is the number of FFT points (the file should have enough samples)
hp_cut_off = float(sys.argv[4]) #Argument 4 is the HP cutt-off frequency in Khz
lp_cut_off = float(sys.argv[5]) #Argument 5 is the LP cutt-off frequency in Khz

if lp_cut_off >= 2.0 and hp_cut_off >= 2.0:
    file_csv = pandas.read_csv(file_name, nrows=FFT_PT)

    matrix = file_csv[file_csv.columns[0]].to_numpy()
    time_domain_data = matrix.tolist()

    #Convert to mg
    time_domain_data = [i * 1000 for i in time_domain_data] 


    #Run Real FFT
    freq_data = rfft(time_domain_data)

    #Extract Magnitude 
    freq_data = numpy.absolute(freq_data)

    #kill DC term 
    freq_data[0] = 0
    #Descale to get magnitude in mg
    freq_data = [i * 2.0 / FFT_PT for i in freq_data]
    freq_steps = rfftfreq(len(freq_data), d=1./Fs)

    matplotlib.pyplot.plot(time_domain_data)
    matplotlib.pyplot.title('Time Domain Data')
    matplotlib.pyplot.ylabel("Acceleration (mg)")
    matplotlib.pyplot.xlabel("Time (n)")

    matplotlib.pyplot.figure()
    matplotlib.pyplot.title('Frequency Domain Data')
    matplotlib.pyplot.plot(freq_steps, freq_data)
    matplotlib.pyplot.ylabel("Acceleration (mg)")
    matplotlib.pyplot.xlabel("Frequency (Hz)")

    filtered_signal = butter_bandpass_filter(time_domain_data, hp_cut_off * 1000.0, lp_cut_off * 1000.0, Fs, order=6)

    #Run Real FFT
    freq_data = rfft(filtered_signal)

    #Extract Magnitude 
    freq_data = numpy.absolute(freq_data)
    #Descale to get magnitude in mg
    freq_data = [i * 2.0 / FFT_PT for i in freq_data]

    matplotlib.pyplot.figure()
    matplotlib.pyplot.title('Filtered Frequency Domain Data')
    matplotlib.pyplot.plot(freq_steps, freq_data)
    matplotlib.pyplot.ylabel("Acceleration (mg)")
    matplotlib.pyplot.xlabel("Frequency (Hz)")

    #Applying Hilbert's transform 
    hilbert_signal = numpy.abs(hilbert(filtered_signal))
    hilbert_signal_no = hilbert_signal - numpy.mean(hilbert_signal)

    #Run Real FFT
    freq_data = rfft(hilbert_signal_no)

    #Extract Magnitude 
    freq_data = numpy.absolute(freq_data)
    freq_data = [i * 2.0 / FFT_PT for i in freq_data]
    matplotlib.pyplot.figure()
    matplotlib.pyplot.title('Envelope')
    matplotlib.pyplot.plot(freq_steps, freq_data)
    matplotlib.pyplot.ylabel("Acceleration (mg)")
    matplotlib.pyplot.xlabel("Frequency (Hz)")


    matplotlib.pyplot.show()

else:
    print("Error : Lp & Hp filters must be >= 2 kHz")
