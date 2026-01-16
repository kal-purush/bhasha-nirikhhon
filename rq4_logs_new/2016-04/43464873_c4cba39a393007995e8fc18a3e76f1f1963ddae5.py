#!/usr/bin/env python
"""Sample code to test json2vega.py.

Author: Farmehr Farhour f.farhour@gmail.com.

Contains sample code that calls the functions in json2vega.py as intended.

Usage: simply run the script to create a sample bar graph.
"""

import os,argparse,sys			#built-in
import json2vega

#class: bcolors
#Class for coloring terminal outputs using ANSI color codes
class bcolors:
	"""Used to implement ANSI colors without the need to remember the numbers.

	Does not contain any methods. Only contains variables.

	Usage: simply concatenate bcolors.<color> at the start of string to be printed,
		and bcolors.ENDC at the end of the string, to color the string with the
		specified color.
	"""

	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKGREEN = '\033[92m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	ENDC = '\033[0m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'


#function: inputArgs
#Processes input arguments
def inputArgs(argv):
	"""function to process input args.

	The various input arguments are defined here. parser.add_argument creates a new
		argument.
	The function parses the various input args passed, and stores them in a list (args).
	The argeparse library automatically takes care of the help command.
	Args:
		argv: the input arguments passed through commandline. Simply pass in sys.argv from
			the main function.

	Returns:
		args: a  populated namespace of all the inputted commandline arguments.
	"""
	#parse args
	parser = argparse.ArgumentParser(description=bcolors.HEADER + 'IO Options' + bcolors.ENDC)
	# add the arguments available to user
	parser.add_argument('-d', metavar='<directory>', type=str, help='directory containing input JSON files. Default = /', default='../gunrock-output/')
	parser.add_argument('-o', metavar='<directory>', type=str, help='directory for output files. Default = output/', default='output/')
	#TODO add additional arguments: engine_name, algorithm_name,axes_vars,conditions,config_path
	args = parser.parse_args()
	return args

def main(argv):
	"""Creates a bar graph by calling methods in json2vega.py"""

    #PRINT HEADERS in commandline
	print bcolors.HEADER + __doc__ + bcolors.ENDC
	#print bcolors.HEADER + "Author: Farmehr Farhour f.farhour@gmail.com" + bcolors.ENDC

	args = inputArgs(argv)			#process input arguments passed
	if not os.path.exists(args.o):			#create output directory
		os.makedirs(args.o)

	# TESTS
	# Call json2vega.py's docstring
	print bcolors.OKBLUE + json2vega.__doc__ + bcolors.ENDC

    # Create required arguments and instantite bar class object for testing.
	conditions = {"algorithm" : "BFS","undirected" : True ,"mark_predecessors" : True}
	axes_vars = {'x':'dataset','y':'m_teps'}
	bar1 = json2vega.VegaGraphBar(args.o,args.d,"config_files/","g","BFS",conditions,axes_vars)
	bar1.read_json()
	bar = bar1.parse_jsons()
	bar1.write_json(bar,"0")


if __name__ == "__main__":
  	main(sys.argv)