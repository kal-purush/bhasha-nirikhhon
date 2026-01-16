import sys

def main(filename):
	with open(filename, 'r') as f:
		for line in f:
			swapped = [word[-1]+word[1:-1]+word[0] for word in line.strip().split()]
			print " ".join(swapped)


if len(sys.argv) != 2:
	print "Usage:", sys.argv[0], "<filename>"
	sys.exit(1)
main(sys.argv[1])