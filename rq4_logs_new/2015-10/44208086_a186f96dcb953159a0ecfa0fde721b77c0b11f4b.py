#!/usr/bin/env python

import numpy, csv
from numpy.lib import recfunctions


# Set user variables, this will migrate to command line arg
input_path = "/home/ygritte/github/Timber-Calculations/sample_data/ExampleA-TreeSpeciesDBH.csv"
output_path = "/home/ygritte/github/Timber-Calculations/tmp.csv"
delimiter = ","
plot_type = ""
unit_type = ""


def import_data(path,delimiter):
	# Generate numpy array from input
	return numpy.genfromtxt(path,dtype=None,delimiter=delimiter,filling_values=0,names=True,case_sensitive="lower",autostrip=True)

def export_data(path,data):
	# Open file
	out_csv = csv.writer(open(path,"w"), dialect="excel")
	# Write field names then the data
	out_csv.writerow(list(data.dtype.names))
	out_csv.writerows(data.tolist())
	return

def basalarea_sqft(data,dbh):
	ba = ((numpy.pi*(data[dbh]**2))/576)
	return recfunctions.append_fields(data,"basal_area",ba)


# Calculate Basal Area in SqFt Example (Replace the basalarea_sqft function with what ever funciton is being tested)
data = import_data(input_path,delimiter)  # Import the data from csv
data = basalarea_sqft(data,"dbh")  # Calculate basal area using the "dbh" field
export_data(output_path, data)  # Export the newly edited data to csv
""" 
	Optionally you could use:
	export_data(output_path, basalarea_sqft(import_data(input_path,delimiter),"dbh"))
	but this is fairly confusing to interpret.
"""
