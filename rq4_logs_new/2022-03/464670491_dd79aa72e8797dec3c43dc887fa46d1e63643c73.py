##Takes in daily Little Rock area temperatures from 2000 to 2022, graphs, and gets major temp events.
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

#Opens text file, sorts day data line by line into array, sorts array into huge array
def sort_data():
    all_data = []
    with open("/home/julia/Textfiles/temp_data.txt", 'r') as file:
        for line in file:
            day = [item.strip() for item in line.split()]
            all_data.append(day)

    #Neats up list, removing \t and \n
    all_data = [item for item in all_data if item not in ['\n', '\t']]

    #Converts high, low, and averages into int, int, and float respectively
    all_data = [list(map(int, item) for item in all_data[:][0:3])]
    
    print(all_data)
    #Stores dates, high, low, and average temp data into seperate numpy arrays
    dates = []
    highs = np.empty(1, dtype = int)
    lows = np.empty(1, dtype = int)
    averages = np.empty(1)

    for value in all_data:
        dates.append(all_data[0])
        highs = np.append(all_data[1], highs)
        lows = np.append(all_data[2], lows)
        averages = np.append(all_data[3], averages)
        
def main():
    weather_arrays = sort_data()
    
main()
