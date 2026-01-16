#!/usr/local/bin/python3
import xlrd as exc
import csv
import argparse

def parse_args():
	parser=argparse.ArgumentParser( description ='supply file that need to be parsed')
	parser.add_argument('--filename',required=True)
	parser.add_argument('--outputname',required=True)
	return parser.parse_args()


def csvconvert(filename,outputname):
	workbook = exc.open_workbook(filename)
	intial_li=[]
	for i in workbook.sheet_names():
		processing_sheet=i
		worksheet = workbook.sheet_by_name(processing_sheet)
		for j in range(0, worksheet.nrows):
			initial_li=intial_li.append([i]+worksheet.row_values(j)) 
	#print(intial_li)
	with open(outputname, "w") as f:
    		writer = csv.writer(f,delimiter='~')
    		writer.writerows(intial_li)


def main():
	args = parse_args()
	csvconvert(args.filename,args.outputname)


if __name__ == "__main__":
	main()	