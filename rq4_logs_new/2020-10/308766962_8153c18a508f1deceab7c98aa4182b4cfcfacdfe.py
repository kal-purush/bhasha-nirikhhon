#!/usr/bin/python

import csv, re
import sys, getopt

# Formato das linhas.
#exp = schedule-test, execution = 1, benchmark = gemm, size_of_data = LARGE_DATASET, schedule = DYNAMIC, chunk_size = 16, num_threads = 1,
#version = OMP, num_threads = 1, NI = 2000, NJ = 2000, NK = 2000, ORIG = 0, OMP = 74536668053, 
#74.536703

header = 'exp,execution,benchmark,size_of_data,schedule,chunk_size,num_threads,version,num_threads,NI,ORIG,OMP'

def parse(data):

   print ('parsing...')

   result = re.findall('exp = (.*?), execution = (.*?), benchmark = (.*?), size_of_data = (.*?), schedule = (.*?), chunk_size = (.*?), num_threads = (.*?),\n.*?\n*version = (.*?), num_threads = (.*?), NI = (.*?), ORIG = (.*?), OMP = (.*?), \n', data, re.DOTALL)

   print (result)

   return result

def write_to_csv(parsed_data, header, filename):
   with open(filename, 'w') as f:
      f.write(header + '\n')
      writer = csv.writer(f, lineterminator='\n')
      for item in parsed_data:
         writer.writerow(item)

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print ('csvize.py -i <inputfile> -o <outputfile>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print ('csvize.py -i <inputfile> -o <outputfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
   
   if inputfile and outputfile :
      print ('Input file is "', inputfile)
      print ('Output file is "', outputfile)
      with open(inputfile, 'r') as f:
    	   data = f.read()
    	   print (data)
      result = parse(data)
      print ('writing...')
      write_to_csv(result, header, outputfile)
   else :
      print ('Use:\ncsvize.py -i <inputfile> -o <outputfile>')

if __name__ == "__main__":
   main(sys.argv[1:])