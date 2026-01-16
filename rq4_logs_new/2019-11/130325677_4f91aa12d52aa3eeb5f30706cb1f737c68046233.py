#!/usr/bin/env python
from datetime import datetime,date,time
#from time import sleep
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
#import serial
import time
#import xlrd
#from xlrd import xldate
import re
import sys
#import statistics
from xmlConversion import generateXMLHeader, generateDataSet, writeToFile,writeToFile1
from xmlConversion import generateXMLDataSCInstStatus


#SUP CHAMB WHEEL LOC
def xml_data_generation(excel_file):
	location=sys.argv[2]
	super_chamber = sys.argv[3]
	cooling_connected = sys.argv[4]
        lv_connected = sys.argv[5]
        hv_connected= sys.argv[6]
	fibers_connected= sys.argv[7]
	gas_connected= sys.argv[8]
        daq_connected= sys.argv[9]
	install_date= sys.argv[10]
	root = generateXMLHeader("SC_INSTALLATION_STATUS","SUPER CHAMBER INSTALLATION STATUS","SUPER CHAMBER INSTALLATION STATUS","","","","","","")
	dataSet = generateDataSet(root,"","1","GEM Super Chamber",super_chamber)	
	generateXMLDataSCInstStatus(dataSet,str(location),str(super_chamber),str(cooling_connected), str(lv_connected),str(hv_connected),str(fibers_connected),str(gas_connected),str(daq_connected),str(install_date))
	writeToFile(fileName, tostring(root))


if __name__ =="__main__":	
	#fname=raw_input("Enter excel data file name:")
	fname = sys.argv[1]
	fileName=fname+".xml"
	#datafile=fname+"_Data.xml"
	#testfile=fname+"_summry.xml"
	print fileName
	xml_data_generation(fname)	