#Import Python modules for handling csv
import sys
import csv

#Open phonebook.csv file
myfile = csv.reader(open("phonebook.csv", 'rU'), delimiter=",", dialect=csv.excel_tab)

#Return information from file
for first_name, last_name, phone1 in myfile:
    print first_name