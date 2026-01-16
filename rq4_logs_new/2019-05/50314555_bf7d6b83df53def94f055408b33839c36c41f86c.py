"""
Collects all FFR level rank data (excluding token level ranks) given a
user's credentials and stores the results in 'output_file'. The reuslts
are ordered so that the easiest and lowest ranking levels that still
need to be AAA'd appear first.

Assumes there is an existing file in this directory named 'credentials'
that contains a username and password in JSON format:
{"username":"YOUR_USERNAME","password":"YOUR_PASSWORD"}
"""

from bs4 import BeautifulSoup
from collections import OrderedDict
from tabulate import tabulate
import json
import mechanize
import re
import sys

output_file = 'level_ranks.txt'

def get_data(credentials, stats_username):
	"""
	Given a credentials object, returns all level rank data for 'stats_username'.
	"""
	url_base = 'http://www.flashflashrevolution.com'
	url_levelrank = url_base + '/levelrank.php?sub=' + stats_username

	# initialize mechanize browser
	br = mechanize.Browser()
	br.set_handle_robots(False)
	br.addheaders = [('User-agent', 'Mozilla/5.0')] 

	print('[+] GET ' + url_base)
	br.open(url_base)

	print('[+] Logging in with credentials...')
	br.form = br.forms()[0]
	br.form['vb_login_username'] = credentials['username']
	br.form['vb_login_password'] = credentials['password']
	br.submit()

	print('[+] GET ' + url_levelrank)
	data = br.open(url_levelrank)
	# print(data.read())

	soup = BeautifulSoup(data, 'html.parser', from_encoding='iso-8859-1')

	if soup.table == None:
		print("[+] ERROR: Invalid login credentials")
		exit()

	return soup.table

def format_data(raw_data):
	"""
	Given raw level rank data, returns a list of OrderedDict objects containing the
	difficulty, rank, and name of all levels that have not been AAA'd.
	"""

	print('[+] Formatting level ranks...')

	# get table columns
	cols = {}
	for i, th in enumerate(raw_data('tr')[0]('th')):
		if th.span:
			# remove arrow from table headers
			th.span.extract()
		cols[th.string] = i

	levels = []

	for tr in raw_data('tr')[1:]:
		row = tr('td')
		# skip entries that are already AAA'd
		rank = int(row[cols['Rank']].string.replace(',', ''))
		if rank != 1:
			# extract meaningful data for each level
			difficulty = int(row[cols['D']].string)
			name = row[cols['Level']].string

			levels.append(OrderedDict([('D', difficulty),
				('Rank', rank),
				('Name', name)]))

	return levels

credentials = json.loads(open('credentials', 'r').read())

# get the username that the stats will be retrieved for
stats_username = credentials['username']
if len(sys.argv) == 2:
	stats_username = sys.argv[1]
elif len(sys.argv) != 1:
	print('Invalid argument format. Please call this script with the following format:')
	print('\tpython3 stats.py <OPTIONAL:stats_username>')
	sys.exit()

raw_data = get_data(credentials, stats_username)
levels = format_data(raw_data)

# sort the level data primarily by difficulty, and secondly by lowest rank
sorted_levels = sorted(levels, key=lambda level:level['Rank'])
sorted_levels = sorted(sorted_levels, key=lambda level:level['D'])

with open(output_file, 'w') as f:
    f.write(tabulate(sorted_levels, headers='keys', tablefmt='simple'))

print('[+] Level ranks written to ' + output_file)