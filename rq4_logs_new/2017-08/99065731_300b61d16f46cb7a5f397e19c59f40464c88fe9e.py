import os
import sys
import getopt

def add_color(string, color):
	"""
	Add color to a string for console output
	"""
	COLORS = {
		'red': '\033[0;31m',
		'green': '\033[0;32m',
		'yellow': '\033[0;33m',
		'cyan': '\033[0;36m',
	}

	if color not in COLORS.keys():
		raise IndexError("Invalid color")

	color_code = COLORS[color]
	no_color = '\033[0m'

	return color_code + string + no_color

def get_sizes_start(start_path, total_size):
	"""
	Finds the size of the starting directory, and returns a list of all directories
	and files within the starting directory. The lists contain tuples of the format
	(name, size)

		-start_path: Path to begin the walk
		-total_size: A single element list for size, simulates pass by reference
	"""
	directories = []
	files = []

	nodes = list(os.walk(start_path))[0]

	dirpath, dirnames, filenames = nodes

	for f in filenames:
		fp = os.path.join(dirpath, f)
		total_size[0] += os.path.getsize(fp)
		files.append((fp, os.path.getsize(fp)))

	for d in dirnames:
		dp = os.path.join(dirpath, d)
		get_sizes_recursive(dp, directories, files, total_size)

	return total_size[0], directories, files

def get_sizes_recursive(start_path, directories, files , total_size):
	"""
	Recursive helper function to get directory and file lists
	"""
	nodes = list(os.walk(start_path))[0]

	size = 0
	dirpath, dirnames, filenames = nodes

	for f in filenames:
		fp = os.path.join(dirpath, f)
		size += os.path.getsize(fp)
		total_size[0] += os.path.getsize(fp)
		files.append((fp, os.path.getsize(fp)))

	for d in dirnames:
		dp = os.path.join(dirpath, d)
		size += get_sizes_recursive(dp, directories, files, total_size)
	
	directories.append((dirpath, size))

	return size


def format_size(size_in_bytes):
	"""
	Converts an integer number of bytes to KB, MB or GB depending on how large
	it is, returns a string with units appended
	"""
	size = None
	units = 'B'

	if size_in_bytes > 1024 ** 3:
		size = float(size_in_bytes)/(1024 ** 3)
		units = 'GB'

	elif size_in_bytes > 1024 ** 2:
		size = float(size_in_bytes)/(1024 ** 2)
		units = 'MB'

	elif size_in_bytes > 1024:
		size = float(size_in_bytes)/(1024)
		units = 'KB'
	else:
		size = size_in_bytes

	return str('{0:.2f}'.format(size)) + ' ' + units

def unformat_size(size):
	"""
	Takes storage size of format "size-units" and converts it to numerical bytes
	"""
	toks = size.split('-')

	size = float(toks[0])

	if toks[1] == 'GB':
		size = size * (1024 ** 3)
	if toks[1] == 'MB':
		size = size * (1024 ** 2)
	if toks[1] == 'KB':
		size = size * 1024

	return size


def main():
	#Parse command line arguments
	opts, args = getopt.getopt(sys.argv[1:], "ec:p:m:")

	count = None
	empty = False
	min_size = None
	start_path = '.'
	total_size = [0]

	for flag, val in opts:
		if flag == '-c':
			count = int(val)
		if flag == '-e':
			empty = True
		if flag == '-p':
			start_path = val
		if flag == '-m':
			min_size = val

	#Get the total size, directory and file lists
	size, dirs, files = get_sizes_start(start_path, total_size)
	empty_dirs = []

	#Sort the directory and file lists by size
	dirs = sorted(dirs, key=lambda d: d[1], reverse=True)
	files = sorted(files, key=lambda f: f[1], reverse=True)

	#Handle passed in flags
	if empty is True:
		for d in dirs:
			if d[1] == 0:
				empty_dirs.append(d)

	if count is not None:
		dirs = dirs[:count]
		files = files[:count]

	if min_size is not None:
		min_size = unformat_size(min_size)
		dirs = [d for d in dirs if d[1] >= min_size]
		files = [f for f in files if f[1] >= min_size]

	#Print out size information
	print("Largest directories:")
	for d in dirs:
		print(add_color(d[0], 'green') + ' | ' + add_color(format_size(d[1]), 'cyan'))
	print("")

	if empty is True:
		print("Empty directories:")
		for ed in empty_dirs:
			print(add_color(ed[0], 'green') + ' | ' + add_color(format_size(ed[1]), 'cyan'))
		print("")

	print("Largest files:")
	for f in files:
		print(add_color(f[0], 'green') + ' | ' + add_color(format_size(f[1]), 'cyan'))

	print("Total size: " + add_color(format_size(size), 'red'))


if __name__ == "__main__":
	main()