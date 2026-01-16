from s5v2 import *
from prettytable import PrettyTable

print("\n s5v3: ")

def my_table():
	x = PrettyTable(['Style', 'Average Prcie'])
	x.add_row(['Print', pretty_average(print_ties)])
	x.add_row(['Solid', pretty_average(solid_ties)])
	x.add_row(['Paisley', pretty_average(paisley_ties)])
	x.add_row(['Striped', pretty_average(striped_ties)])
	x.add_row(['Gucci', pretty_average(gucci_ties)])
	print(x)

def pretty_average(my_number):
	pretty_average = "${:03.2f}".format(find_average(my_number))
	return pretty_average

def count_prices_for_brands(lst, bran, min_price, max_price):
	count = 0
	for row in data_sample:
		if str(row[0]) == str(brand):
			if min_price < float(row[1]) < max_price:
				count += 1
	return count