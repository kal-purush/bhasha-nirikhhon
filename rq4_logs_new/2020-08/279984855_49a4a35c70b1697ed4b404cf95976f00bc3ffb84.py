
from scipy.fft import fft, ifft
import csv
import numpy as np
import matplotlib.pyplot as plt

def get_elevations(data_path, buttock):
    """Find elevations for given run at given buttock"""
    
    with open(data_path, 'r') as data:
        data_reader = csv.reader(data, delimiter=',')
        steady_data = list(data_reader)[400:1100]
    steady_data = [map(float, row) for row in steady_data]
    steady_data = [list(row) for row in steady_data]
    
    elevations = [row[buttock] for row in steady_data]
    return elevations

elevations = get_elevations("../data/2016-06-29_T5/TR5-R1.94A1V.csv", 3)
x = np.array(elevations)
y = fft(x)

print(y[0])
plt.plot(y)
plt.xlim(0, 60)
plt.show()