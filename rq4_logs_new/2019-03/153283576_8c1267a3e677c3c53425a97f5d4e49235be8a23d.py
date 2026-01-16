#Finding the max DM and time in the dataset

import numpy as np
import pandas as pd
import glob, os

#fills data frame
def DF(path):
    axislabels = ["DM", "Time", "S/N", "Width"]
    Tfile = open(path,'r')
    data = np.fromfile(Tfile,np.float32,-1) 
    c = data.reshape((-1,4))
    df = pd.DataFrame(c,columns=axislabels)
    Tfile.close()
    return df

#array of file locations and chosing the file to inspect with path
source_paths = []

#filling source_paths from the idir
for file in glob.glob(os.getcwd() + '\idir\\' + "*.dat"):
    source_paths.append(file)

#max value arrays
time_max = []
dm_max = []

for i in range(73,98): 
    
    path=i
    print(i)
    #setting which file to open
    FILE = source_paths[path]
    #getting df for test file
    df = DF(FILE)
    
    #construct array of max values for each file
    dm_max.append(df["DM"].max())
    time_max.append(df["Time"].max())
    
    #scatter plot file
    df.plot(x="Time", y="DM", kind="scatter")
    """
    df.to_csv('File_34.txt',index=False)
    
    DM = np.array(df["DM"])
    U_DM=np.unique(DM)
    F=np.round(U_DM,decimals=3)
    print(F)
    np.savetxt("unique_34.txt",F,fmt="%.3f")
    """
    time_all = np.array(df["Time"])
#printing max array
print("DM maxes for all files: \n", dm_max, "\n")
print("Time maxes for all files: \n", time_max, "\n")

#finding the max for all files and its index
dm_amax = max(dm_max)
time_amax=argmax = max(time_max)
dm_imax = np.array(dm_max).argmax()
time_imax=argmax = np.array(time_max).argmax()

#output
print("Absolute max in files: \nDM:", dm_amax," file:",dm_imax,"\nTime: ", time_amax," file:",time_imax, "\n")

"""
print(len(time_all))
print(time_all)
"""

#time steps experimentation
print("Time step multiple stuff:")
U_ta = np.unique(time_all)
print(len(U_ta))
print(U_ta[4])
print(U_ta[0:15]/(256.0*(pow(10,-6))))
print(256*(pow(10,-6)))
a=432*(256*(pow(10,-6)))
b=round(a,6)
print(a)
print(b)