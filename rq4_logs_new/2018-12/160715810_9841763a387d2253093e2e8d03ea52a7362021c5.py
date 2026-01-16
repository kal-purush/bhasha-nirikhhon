#!/usr/bin/python3

from time import sleep, time
from random import randint

def print_num(elem, val1):
	print('Function call %d - Random Value is: %d'%(elem,val1))
	print('Function call %d - sleeping for %d seconds'%(elem,val1))
	sleep(val1)
	print()