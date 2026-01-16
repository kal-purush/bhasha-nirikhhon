#!/bin/env python
# add your header here
# Author: kushpaliwal

# Created On: 20th April 2020

# Script to check the given dataset (DataQualityChecking.txt) and remove no data values, gross errors, inconsistencies in variables, and range problems

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def ReadData( fileName ):
    """This function takes a filename as input, and returns a dataframe with
    raw data read from that file in a Pandas DataFrame.  The DataFrame index
    should be the year, month and day of the observation.  DataFrame headers
    should be "Date", "Precip", "Max Temp", "Min Temp", "Wind Speed". Function
    returns the completed DataFrame, and a dictionary designed to contain all 
    missing value counts."""
    
    # define column names
    colNames = ['Date','Precip','Max Temp', 'Min Temp','Wind Speed']

    # open and read the file
    DataDF = pd.read_csv("DataQualityChecking.txt",header=None, names=colNames,  
                         delimiter=r"\s+",parse_dates=[0])
    DataDF = DataDF.set_index('Date')
    
    # define and initialize the missing data dictionary
    ReplacedValuesDF = pd.DataFrame(0, index=["1. No Data"], columns=colNames[1:])
     
    return( DataDF, ReplacedValuesDF )
 
def Check01_RemoveNoDataValues( DataDF, ReplacedValuesDF ):
    """This check replaces the defined No Data value with the NumPy NaN value
    so that further analysis does not use the No Data values.  Function returns
    the modified DataFrame and a count of No Data values replaced."""

    # Replace -999 to be np.NaN
    DataDF = DataDF.replace(-999, np.nan)
    
    # Count the NaN values of each data type
    for i in ReplacedValuesDF:
        ReplacedValuesDF[i] = len(DataDF) - DataDF[i].count()
    
    return( DataDF, ReplacedValuesDF )
    
def Check02_GrossErrors( DataDF, ReplacedValuesDF ):
    """This function checks for gross errors, values well outside the expected 
    range, and removes them from the dataset.  The function returns modified 
    DataFrames with data the has passed, and counts of data that have not 
    passed the check."""
    
    # add your code here
    
    # Precipitation Error Threshold
    DataDF['Precip'] = DataDF['Precip'].mask(DataDF['Precip']>25, np.nan)
    DataDF['Precip'] = DataDF['Precip'].mask(DataDF['Precip']<0, np.nan)
    
    # Temperature Error Threshold
    DataDF['Max Temp'] = DataDF['Max Temp'].mask(DataDF['Max Temp']<-25, np.nan)
    DataDF['Max Temp'] = DataDF['Max Temp'].mask(DataDF['Max Temp']>35, np.nan)
    DataDF['Min Temp'] = DataDF['Min Temp'].mask(DataDF['Min Temp']<-25, np.nan)
    DataDF['Min Temp'] = DataDF['Min Temp'].mask(DataDF['Min Temp']>35, np.nan)
    
    # Windspeed Error Threshold
    DataDF['Wind Speed'] = DataDF['Wind Speed'].mask(DataDF['Wind Speed']<0, np.nan)
    DataDF['Wind Speed'] = DataDF['Wind Speed'].mask(DataDF['Wind Speed']>10, np.nan)
    
    # Add row '2. Gross Error'
    ReplacedValuesDF.loc['2. Gross Error'] = 0
    
    # Count number of out of range values and have been replaced by np.nan
    ReplacedValuesDF.loc['2. Gross Error'] = DataDF.isnull().sum() - ReplacedValuesDF.loc['1. No Data'] 
        
    return( DataDF, ReplacedValuesDF )
    
def Check03_TmaxTminSwapped( DataDF, ReplacedValuesDF ):
    """This function checks for days when maximum air temperture is less than
    minimum air temperature, and swaps the values when found.  The function 
    returns modified DataFrames with data that has been fixed, and with counts 
    of how many times the fix has been applied."""
    
    # add your code here
    
    # Loop for swapping temperature values
    count = 0
    min_temp = 0
    for i in range(len(DataDF)):
        if DataDF['Max Temp'][i] < DataDF['Min Temp'][i]:
            min_temp = DataDF['Max Temp'][i]
            DataDF['Max Temp'][i] = DataDF['Min Temp'][i]
            DataDF['Min Temp'][i] = min_temp
            count += 1
    
    # Add row to count swapped values       
    ReplacedValuesDF.loc['3. Swapped'] = 0 
    
    # Count swapped value
    ReplacedValuesDF.loc['3. Swapped'][1:3] = count        
    
    return( DataDF, ReplacedValuesDF )
    
def Check04_TmaxTminRange( DataDF, ReplacedValuesDF ):
    """This function checks for days when maximum air temperture minus 
    minimum air temperature exceeds a maximum range, and replaces both values 
    with NaNs when found.  The function returns modified DataFrames with data 
    that has been checked, and with counts of how many days of data have been 
    removed through the process."""
    
    # add your code here
    count = 0
    for i in range(len(DataDF)):
        if DataDF['Max Temp'][i] - DataDF['Min Temp'][i] > 25:
            DataDF['Max Temp'][i] = np.nan
            DataDF['Min Temp'][i] = np.nan
            count += 1
            
    # Add row to count number of values exceeding the range       
    ReplacedValuesDF.loc['4. Range Fail'] = 0 
    
    # Count values exceeding range value
    ReplacedValuesDF.loc['4. Range Fail'][1:3] = count  
       
    return( DataDF, ReplacedValuesDF )
    

# the following condition checks whether we are running as a script, in which 
# case run the test code, otherwise functions are being imported so do not.
# put the main routines from your code after this conditional check.

if __name__ == '__main__':

    fileName = "DataQualityChecking.txt"
    DataDF, ReplacedValuesDF = ReadData(fileName)
    DataDF_origin = DataDF
    
    print("\nRaw data.....\n", DataDF.describe())
    
    DataDF, ReplacedValuesDF = Check01_RemoveNoDataValues( DataDF, ReplacedValuesDF )
    
    print("\nMissing values removed.....\n", DataDF.describe())
    
    DataDF, ReplacedValuesDF = Check02_GrossErrors( DataDF, ReplacedValuesDF )
    
    print("\nCheck for gross errors complete.....\n", DataDF.describe())
    
    DataDF, ReplacedValuesDF = Check03_TmaxTminSwapped( DataDF, ReplacedValuesDF )
    
    print("\nCheck for swapped temperatures complete.....\n", DataDF.describe())
    
    DataDF, ReplacedValuesDF = Check04_TmaxTminRange( DataDF, ReplacedValuesDF )
    
    print("\nAll processing finished.....\n", DataDF.describe())
    print("\nFinal changed values counts.....\n", ReplacedValuesDF)
    
    # Plot original and Corrected data
    # Precipitation data
    plt.figure(figsize=(9, 5))
    plt.plot(DataDF.index, DataDF_origin['Precip'], label='Original')
    plt.plot(DataDF.index, DataDF['Precip'], label='Checked')
    plt.title('Preciptation')
    plt.xlabel('Time')
    plt.ylabel('Precipitation (mm)')
    plt.legend()
    plt.savefig("Corrected Precipitation.jpeg")
    plt.show()
    plt.close()
    
    # Max Temperature data
    plt.figure(figsize=(9, 5))
    plt.plot(DataDF.index, DataDF_origin['Max Temp'], label='Original')
    plt.plot(DataDF.index, DataDF['Max Temp'], label='Checked')
    plt.title('Maximum Temperature')
    plt.xlabel('Time')
    plt.ylabel('Max temperature (°C)')
    plt.legend()
    plt.savefig("Corrected Max Temperature.jpeg")
    plt.show()
    plt.close()
    
    # Min Temperature data
    plt.figure(figsize=(9, 5))
    plt.plot(DataDF.index, DataDF_origin['Min Temp'], label='Original')
    plt.plot(DataDF.index, DataDF['Min Temp'], label='Checked')
    plt.title('Minimum Temperature')
    plt.xlabel('Time')
    plt.ylabel('Min temperature (°C)')
    plt.legend()
    plt.savefig("Corrected Min Temperature.jpeg")
    plt.show()
    plt.close()
    
    # Wind Speed data
    plt.figure(figsize=(9, 5))
    plt.plot(DataDF.index, DataDF_origin['Wind Speed'], label='Original')
    plt.plot(DataDF.index, DataDF['Wind Speed'], label='Checked')
    plt.title('Wind Speed')
    plt.xlabel('Time')
    plt.ylabel('Wind Speed (m/sec)')
    plt.legend()
    plt.savefig("Corrected Wind Speed.jpeg")
    plt.show()
    plt.close()
    
    # Save modified data into another file
    DataDF.to_csv('Modified data.txt', header = None, sep=' ') 
    ReplacedValuesDF.to_csv('Error Check.txt', sep='\t')