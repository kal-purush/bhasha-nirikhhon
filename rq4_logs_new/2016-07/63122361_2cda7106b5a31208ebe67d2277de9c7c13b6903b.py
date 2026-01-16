'''
Insight Data Science Coding Challenge Solution
Author: Jiyoung Park
'''

import os
import sys
import json
import time
import datetime
import networkx as nx 				# non-standard library

# Parameters
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIME_WINDOW = 60

'''
class to process venmo payments and calculate median degree
'''
class ProcessPayment(object):

	'''
	instantiate graph class and instance attributes
	'''
	def __init__(self):
		self.graph = nx.Graph()
		self.time_updated = False
		self.time_max = datetime.datetime.fromtimestamp(0)	# initialize to Jan 1st 1970
		self.time = None
		self.node_0 = None
		self.node_1 = None

	'''
	check and update attributes if data entry is valid
	'''
	def check_input(self, line):
		try:
			# load data
			data = json.loads(line)
			self.node_0, self.node_1, time_str = data['target'], data['actor'], data['created_time']

			# parse time into datetime object
			self.time = datetime.datetime.strptime(time_str, TIME_FORMAT)
			
			# check for complete payment info
			if not self.node_0 or not self.node_1 or not self.time:
				raise Exception('Incomplete payment data')

			return True
		except Exception as e: # I would print error to debug if necessary
			return False

	'''
	check if current time is within time window of maximum running time
	'''
	def within_time_window(self, time):
		time_elapsed = self.time_max - time

		return time_elapsed < datetime.timedelta(seconds=TIME_WINDOW)
		
	'''
	add or update existing payment to graph if payment is valid
	'''
	def process_payment(self):
		# check time_max has been changed and update time_max
		if self.time_max >= self.time:
			self.time_updated = False
		else:
			self.time_max = self.time
			self.time_updated = True

		# add to graph and return true if its time is within window
		if self.within_time_window(self.time):
			# add_edge() updates edge if it already exists and creates new nodes if they don't exist
			self.graph.add_edge(self.node_0, self.node_1, {'created_time': self.time})

	'''
	prune outdated payments from graph
	'''
	def prune_payments(self):
		if self.time_updated:
			for edge in self.graph.edges(data='created_time'):
				node_0, node_1, time = edge[0], edge[1], edge[-1]

				if not self.within_time_window(time):
					self.graph.remove_edge(node_0, node_1)
					# also remove nodes if they no longer have any neighbors
					if len(self.graph.neighbors(node_0)) == 0:
						self.graph.remove_node(node_0)
					if len(self.graph.neighbors(node_1)) == 0:
						self.graph.remove_node(node_1)

	'''
	calculate median degree
	'''
	def get_median_degree(self):
		# get list of all degrees (number of neighbors) and sort in ascending order
		degrees = sorted([len(self.graph.neighbors(node)) for node in self.graph.nodes()])

		# handle even or odd number list of degrees
		mid = len(degrees) / 2
		if len(degrees) % 2 == 0:
			median = (degrees[mid-1] + degrees[mid]) / 2.0	# even
		else:
			median = degrees[mid]							# odd

		# round to 2 decimal places
		self.median = round(median, 2)

	'''
	save median degree to file
	'''
	def save_to_file(self, file_path):
		with open(file_path, 'a') as f:
			f.write('{:.2f}\n'.format(self.median))

def main(path_input, path_output):
	# delete existing output file
	if os.path.isfile(path_output):
		os.remove(path_output)

	# instantiate class object
	payment = ProcessPayment()	

	# process input file line by line
	with open(path_input, 'r') as f:
		for line in f:
			# process rest of methods only if input is valid
			if payment.check_input(line):
				payment.process_payment()
				payment.prune_payments()
				payment.get_median_degree()
				payment.save_to_file(path_output)

if __name__ == '__main__':
	if len(sys.argv) != 3:
		print('Usage: python ./src/rolling_median.py ./venmo_input/venmo-trans.txt ./venmo_output/output.txt')
	else:
		main(sys.argv[1], sys.argv[2])