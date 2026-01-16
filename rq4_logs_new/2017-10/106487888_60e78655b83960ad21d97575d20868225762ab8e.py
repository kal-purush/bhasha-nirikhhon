import csv
import os
filename = "top-1m.csv"

rows = []
itemslist = []
#reading csv file
with open(filename, 'r') as csvfile:
        #creating csv reader
        csvreader = csv.reader(csvfile)

        #extracting each data row one by one
        for row in csvreader:
            rows.append(row)

        #printing the first 5 rows
        print('\nFirst 10 rows are:\n')
        for row in rows[:10]:
            itemslist.append(row)
            #parsing each column of a row
            for col in row:
                print("%10s"%col),
            print('\n')
print
print "list of the first 10 items: "
print itemslist

filenameAlexa = "top-1m.csv"

def convert_bytes(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def file_sizenum(filenameAlexa):
    if os.path.isfile(filenameAlexa):
        file_info = os.stat(filenameAlexa)
        return convert_bytes(file_info.st_size)
totalfilesize = file_sizenum(filenameAlexa)
print
print
print
print "file size: " + totalfilesize
print "file name: " + filenameAlexa