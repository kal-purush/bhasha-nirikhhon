# Script written for Windows environment since most DMR radios only have windows apps for codeplug manipulation. 
# Written by Ed, KE8TES
# This will work on all talkgroup lists to pad out the Talkgroup number to 7 digits for Custom network configuration. You will be asked to input the number you want as first digit while script is running.
# When creating this script I realized TGIF has some talkgroups with 7 digits already and some of those did not have 5 as their leading digit.
# so I pulled out those with 7 which did not start with the desired leading digit; saving them to error file so they did not corrupt your codeplug in case they were overlooked.
# I did not add header row to the output files as you typically are just pasting the files together anyway.
# Please use and modify as you see fit.

import csv
import os
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()
filename = filedialog.askopenfilename() # Opens file dialog to select your file
filepath = os.path.dirname(filename) # grab path for the original file so output files can be saved to same directory
starting_number = input("What number do you need to lead with for your configuration? TGIF usually is 5 for example: ") # Which DMR network are you configuring? 
# BM does not need changing, TGIF usually is 5 for example. 
savefilename = (filepath + "/TG_" + starting_number + "_list.csv") # saving to same directory as opened file with filename of TG_x_list.csv.
Errorfile = (filepath + "/TG_" + starting_number + "_Errorlog.csv") # Error log file saving to same directory with filename of TG_x_Errorlog.csv.

with open(filename, 'r') as input_file, open("temp.csv", 'w+', newline='') as output_file2, open(savefilename, 'w+', newline='') as output_file: #opening  files to pad TG column, creates a temp.csv file to place bad TG numbers in
    reader = csv.reader(input_file)
    writer = csv.writer(output_file)
    writer = csv.writer(output_file2)

    for row in reader:
        if len(row[0]) < 7:
            row[0] = starting_number + row[0].rjust(6, '0') # if cell is less than 7 add your prefix digit and pad cell to 7
            writer = csv.writer(output_file)
            writer.writerow(row) # write to TG_x_List.csv in same folder as original file
          
        elif len(row[0]) == 7:
            writer = csv.writer(output_file2) # save "bad" talkgroups to temp file
            writer.writerow(row) 
        
        else:
            continue 
            
    input_file.close()
    output_file.close()
    output_file2.close()
    
with open("temp.csv", 'r+') as input_file, open(Errorfile, 'w', newline='') as output_file2, open(savefilename, 'a+', newline='') as output_file: # opening temp file to loop through bad files and find ones that are 7 long and start with your first character
    reader = csv.reader(input_file) # reading the temp file
    writer = csv.writer(output_file)
    writer = csv.writer(output_file2)
    
    for row in reader:
        if row[0].startswith(starting_number):
            writer = csv.writer(output_file)
            writer.writerow(row) # append to TG_x_List.csv in same folder as original file
            
        else:
            writer = csv.writer(output_file2)
            writer.writerow(row) # write to TG_x_Errorlog.csv in same folder as original file
            
            continue
        
os.remove("temp.csv")
    