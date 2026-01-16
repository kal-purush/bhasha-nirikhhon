def find_open_ip(mac):
	import ast
	import json
	import string
	import random

	valid = [i for i in range(20, 250)]
	with open('ip_addresses.json') as address_file:
		dict = json.load(address_file, strict = False)
	for k, v in dict.iteritems():
		last_digit = string.split(v, '.')[-1]
		if mac == k:
			return v
		elif v in valid:
			valid.remove(v)
	dict[mac] = '192.168.2.' + str(random.choice(valid))
	
	with open('ip_addresses.json', 'w') as address_file:
		json.dump(dict, address_file)
	return dict[mac]

if __name__ == '__main__':
	print find_open_ip('02:12:34:56:78:9A')
	print find_open_ip('02:12:34:56:78:9B')