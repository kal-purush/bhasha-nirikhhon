#											o
#                                      /  |/
#                                    _/___|___________
#                                   /  _______      __\
#   _______                        /  /_o_||__|    |
#    \_\_\_\______________________/___             |
#             \                       \____________|______________
#              \     ||    BC FERRIES                             |
#               \  +_||_+       () () ()                      ____|
#                \                                             |
#                 \     _  ,,          _                      /
# ^^^^^^^^^^^^^^^^ \_.=" )"  "-._____,' ";__________________ /_^^^^^^^^
#   ^^^^  ^^^^                                              \__|==% ^^
#  ^^         ^^^^^^^^       ^^^^ ^^^ ^^^^^      ^^^^^^^^^^ ^      ^^^^
#^^^   ^^^^          ^^^^^^^^^^^^          ^^^^     ^^       ^^^^^
#from www.chris.com/ascii/index.php?art=transportation/nautical by sabian@pacbell.net

# Author 			: Corey Koelewyn
# Student ID		: V00869081
# Class and section	: SENG 265 A01
# Last edited		: 11/17/17 
# Last Change 		: implemented Argparsing as final step
# Purpose			: Read through data from BC ferries CSV files to determine
#					  average monthly wait time from departures from either
#					  the twassan terminal or the Swartz Bay terminal

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("csvfiles", nargs="+")
args = parser.parse_args()

print (args.csvfiles)



def delay_calc(ip_file):
	try: #if an incorrect or missing file is passed in, the program terminates
		test = open(ip_file, 'r')
	except IOError:
		print("{} does not exist in this directory".format(ip_file)) 
		exit()
	count = 0
	d = {'s':[0,0] , 't':[0,0]} # s for swartz, t for twassan. first number is total delays, 2nd number is for counting departures
	for line in test:
		#format for each line that we are interested in:
		#[0] depart terminal, [4] month, [6]sched hr, [7] sched min, [11]dept hr, [12] dept min
		if count == 0: # for skipping the first line which has column titles
			count = 1
			continue
		data = line.split(',')
		if count == 1: # for grabbing month number
			month = int(data[4])
			count = 2
		delay = int(data[11])*60 + int(data[12]) - int(data[6])*60 - int(data[7])
		d[(data[0][0].lower())][0] += delay
		d[(data[0][0].lower())][1] += 1
	return [month , round((d['s'][0] / d['s'][1]), 2) , round((d['t'][0] / d['t'][1]), 2)]


def Month_get():
	ip_month = 0
	while (ip_month < 1) or (ip_month > 12):
		ip_month = input("Input a number from 1 to 12 (month):\n")
		if ip_month == 'q' or ip_month == 'Q':
			exit()
		try:
			val = int(ip_month)
		except ValueError:
			ip_month = 0
		ip_month = int(ip_month)
	return ip_month
	
def Ferry_get():
	ip_ferry = 'd' # for default of course!
	while not ((ip_ferry is 't') or (ip_ferry is 's')):
		ip_ferry = input("\nWhich ferry would you like to calculate the delay for? \n(s) for Swartz Bay, (t) for Tsawwassen or (q) to quit:\n")
		if ip_ferry == 'q' or ip_ferry == 'Q':
			exit()
	return ip_ferry
	
dict_names = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}
dic_delay = {}

files = args.csvfiles
for name in files:
	temp_list = delay_calc(name)
	#key is the month number, [1] is Swartz Bay, [2] is Tsawwassen
	dic_delay[temp_list[0]] =  [temp_list[1],temp_list[2]]
	
input_month = 0
input_ferry = 'n'
delay_ferry_key = 0
while(True): #program terminators are in Month_get() and Ferry_get()
	input_ferry = Ferry_get() 
	input_month = Month_get()
	print("RESULTS")
	if(input_ferry is 't'):
		delay_ferry_key = 0
		print("Tsawwassen:")
	if(input_ferry is 's'):
		delay_ferry_key = 1
		print("Swartz Bay:")
	if(int(input_month) in dic_delay):
		print("\tAverage delay for {}: {}".format(dict_names[input_month], dic_delay[input_month][delay_ferry_key]))
	else:
		print("\tNo delay data for {}".format((dict_names[input_month])))
	print("END")
		
	
	