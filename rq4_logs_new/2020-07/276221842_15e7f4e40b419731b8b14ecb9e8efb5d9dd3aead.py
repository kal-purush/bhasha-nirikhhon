#!/usr/bin/env python

def dynamic_fib(n, k):
	calls = {} # Create a map to keep track of recursive calls
	if n == 1 or n == 2: # Base case
		return 1

	# Check if we've already computed this call
	if not (n-1 in calls.keys()): 
		# if not we save it
		calls[n-1] = dynamic_fib(n - 1, k)
	fib_n1 = calls[n-1]
	
	if not (n-2 in calls.keys()):
		# Multiply by k - each old pair produces k new pairs
		calls[n-2] = k*dynamic_fib(n-2, k)
	fib_n2 = calls[n-2]
	return fib_n1 + fib_n2


	# Match each base with its complement
	dna_c = ''.join([COMPLEMENTS[base] for base in dna])

	# Flip the string to get the reverse complement
	return dna_c[::-1]

def main():
    with open('../data/dataset.txt') as dataset:
        n, k = dataset.read().strip().split()

    # Generate and print the reverse complement
    n, k = int(n), int(k)
    fib = dynamic_fib(n, k)

    print(fib)

if __name__ == '__main__':
    main()