from scipy import interpolate
import pandas as pd
import numpy as np 
import glob
import matplotlib.pyplot as plt

#this file is to take multiple excel files that contain the averaged graphs for each picture and puts them together in one graph

def stretch_and_average(arrays): #this function takes multiple arrays and by interpolating values, it makes them all the same length and averages them 
    # Find the maximum length among all arrays
    max_length = max(len(array) for array in arrays)

    # Initialize a list to store the stretched arrays
    stretched_arrays = []

    # Stretch the shorter arrays to match the maximum length
    for array in arrays:
        # Create an interpolation function for the array
        f = interpolate.interp1d(range(len(array)), array, fill_value="extrapolate")

        # Use the interpolation function to stretch the array
        stretched_array = f(np.linspace(0, len(array) - 1, max_length))
        
        # Add the stretched array to the list
        stretched_arrays.append(stretched_array)

    # Calculate the average of the stretched arrays
    average_array = np.mean(stretched_arrays, axis=0)

    return average_array, stretched_arrays


def process_csv_files(pattern): #this function is used to average all the different cells together in one file 
    # Get a list of all CSV files matching the pattern
    csv_files = glob.glob(pattern)

    # Initialize a list to store the arrays
    arrays = []

    # Read each CSV file and create an array
    for file in csv_files:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(file)
        print("file found")
        # Extract the "Intensity" column from the DataFrame
        intensity = df["Intensity"]

        # Convert the intensity column to a numpy array
        array = intensity.to_numpy()

        # Add the array to the list
        arrays.append(array)

    # Call the stretch_and_average function to calculate the results
    results, stretches = stretch_and_average(arrays)

    return results

# Call the process_csv_files function with the desired patterns
results_A = process_csv_files('*A*.csv')
results_B = process_csv_files('*B*.csv')
results_C2 = process_csv_files('*C2*.csv')
results_D2 = process_csv_files('*D2*.csv')

# Put the results in an array
results = [results_A, results_B, results_C2, results_D2]

# Call the stretch_and_average function to calculate the average array
average_array, single_stretched_arrays = stretch_and_average(results)

# Create a figure and axes for the plot
fig, ax = plt.subplots()

# Plot the intensity values for each result with different colours
ax.plot(single_stretched_arrays[0], label='CLK+ PC')
ax.plot(single_stretched_arrays[1], label='CLK- PC')
ax.plot(single_stretched_arrays[2], label='CLK+ ST6KO')
ax.plot(single_stretched_arrays[3], label='CLK- ST6KO')

# Set the plot title and labels
ax.set_xlabel("Pixel Position")
ax.set_ylabel("Average Fluorescent Intensity(mSC35)")

# Add a legend
ax.legend()

# Show the plot
plt.show()