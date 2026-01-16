# Command Line Utility


import argparse
import sys

# argparse help us to make command line utility
# sys help us to print the content in ths console

parser = argparse.ArgumentParser()

# This will make it a command line utility

parser.add_argument("--x", type =float, default =0)

# This will work as a arguement taker when we use it in terminal, "--x" is the name by which it is to be called

# add_arguement() also gets satisfied only with a name by which it should be called here "--x"

parser.add_argument("--o",type=str,default="add")
parser.add_argument("--y", type =float, default =0)

# So, finally here it takes 3 arguements namely --x value --o value --y value

args = parser.parse_args()

# This will compile all the arguements to be taken all at once


def calc(args) :
	
	if args.o == "add" :
		return args.x + args.y


sys.stdout.write(str(calc(args)))

# This will print the result in the console


# To use this we have to go to the directory where this folder is stored

# Then we have to pass the command python CMDutility.py --x value --o value --y value