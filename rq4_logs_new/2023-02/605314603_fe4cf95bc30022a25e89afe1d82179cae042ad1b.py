#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Data preparation script for 2023MLTWO, lecture 02, sound classification
Complete version

Adapted From
Practical Artificial Intelligence with Swift
Tim Nugent
This material may be protected by copyright.

@author: xyi
"""

import os
import shutil
import pandas as pd

#%% create a list that contains all the class names we interested in
# we will see how this is going to be used later
classes_to_include =  [
    'dog', 'rooster', 'pig', 'cow', 'frog', 'cat', 'hen',
    'insects', 'sheep', 'crow']

#%% paste in your audio directory and output directory in the two variables

#option click on the folder to copy the path

#make sure to add a slash in the end

sounds_directory = "/Users/xyi/Desktop/apple-dev-ml/2023MLUnitTwo/data/ESC-50-master/audio/"

# it's okay if this directory does not exist
output_directory = "/Users/xyi/Desktop/apple-dev-ml/2023MLUnitTwo/data/ESC-50-master/audioForCreateML/"

#%% Make output directory if it does not exsit 

# os.makedirs(DIRECTORY_PATH) is a commonly used method for creating directories

try:
    os.makedirs(output_directory)
except OSError:
    if not os.path.isdir(output_directory):
        raise

# GO AND CHECK YOUR output_directory NOW ! MAGIC HAPPENED


#%%
# Make class directories within it
for class_name in classes_to_include:
    class_directory = output_directory + class_name + '/'
    try:
        os.makedirs(class_directory)
    except OSError:
        if not os.path.isdir(class_directory):
            raise


# GO AND CHECK YOUR output_directory NOW ! MAGIC HAPPENED


#%% read CSV file into a pandas' dataframe object, don't get scared by the names
input_classes_filename = '/Users/xyi/Desktop/apple-dev-ml/2023MLUnitTwo/data/ESC-50-master/meta/esc50.csv'

classes_file = pd.read_csv(
    input_classes_filename,
    encoding='utf-8',
    header = 'infer'
)
#use the variable explorer on the right to see the shape of the dataframe 
#2000, 7 (looks just like a big matrix huh)
#guess what do the 2000 and 7 mean

#%% what's in the dataframe using dataframe.head(how many rows from the top to inspect) 
print(classes_file.head(5))


#%% iterate through the lines of the dataframe, examine the first line! 

#use "break" to stop the whole loop and only output the first line
for line in classes_file.itertuples(index = False):
    print(line)
    break
# we see that the first element is the file name and the fourth element is the category

#%% use line[0] and line[3] to retrieve the filename and category
for line in classes_file.itertuples(index = False):
    print(line[0])
    print(line[3])  
    break
#%%
for line in classes_file.itertuples(index = False):
    # step 1. retrieve the category and store it in a variable called fileCategory
    fileCategory = line[3]
   
    # step 2. use if-else conditional to check if the fileCategory is in the list classes_to_include
    if fileCategory in classes_to_include:
    
        # if the above conditional is true, retrieve the fileName and store it in a variable called fileName
        fileName = line[0]
        
        #recall using the operator + to combine two strings?
        #combine the variable sounds_directory and fileName, stored in a new va called file_src
        file_src = sounds_directory + fileName
        
        #do the same as above, combining output_directory, fileCategory, /, file_name and stored in file_dst
        file_dst = output_directory + fileCategory + "/" + fileName
        
        #a special library called shutil to do the copy paste for us
        shutil.copy2(file_src, file_dst)
    

# GO AND CHECK YOUR output_directory NOW ! MAGIC HAPPENED    
    