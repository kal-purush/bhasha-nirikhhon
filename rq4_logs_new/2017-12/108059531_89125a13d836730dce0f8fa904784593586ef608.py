import pybloomfilter
import time
import sys

""" this is based on the algorithm from the Melsted and Pritchard paper"""
def create_count_table(reads, k, bf):
	T = {}
	K = {}
	for read in reads:
		kmers = make_kmers(read, k)
		K[read] = kmers
		for kmer in kmers:
			if kmer in bf:
				if kmer not in T:
					T[kmer] = 0
			else:
				bf.add(kmer)
	for read in reads:
		kmers = K[read]
		for kmer in kmers:
			if kmer in T:
				T[kmer] = T[kmer] + 1
	for kmer in T.keys():
		if T[kmer] == 1:
			del T[kmer]
	return T

def second_count_table(reads, k, bf):
	T = {}
	for read in reads:
		kmers = make_kmers(read, k)
		for kmer in kmers:
			if kmer in bf:
				if kmer not in T:
					T[kmer] = 2
				else:
					T[kmer] = T[kmer] + 1
			else:
				bf.add(kmer)
	return T


def readFastq(filename):
    sequences = []
    qualities = []
    with open(filename) as fh:
        while True:
            fh.readline() # skip name line
            seq = fh.readline().rstrip() # read base sequence
            fh.readline() # skip placeholder line
            qual = fh.readline().rstrip() #base quality line
            if len(seq) == 0:
                break
            sequences.append(seq)
            qualities.append(qual)
    return sequences, qualities

def make_kmers(read, k):
	kmers = []
	for i in range(len(read) - k + 1):
		kmer = read[i:i+k]
		kmers.append(kmer)
	return kmers

def get_count(bf, T, kmer):
	if kmer in T:
		return T[kmer]
	else:
		if kmer in bf: return 1
	return 0

def first_implementation_results(seq, capacity, k):
	bf = pybloomfilter.BloomFilter(capacity, 0.01, None)
	start = time.time()
	counts = create_count_table(seq, k, bf)
	end = time.time()
	this_time = (end - start) * 1000
	size = sys.getsizeof(counts)
	return (this_time, size, counts)

def second_implementation_results(seq, capacity, k):
	bf = pybloomfilter.BloomFilter(capacity, 0.01, None)
	start = time.time()
	counts = second_count_table(seq, k, bf)
	end = time.time()
	this_time = (end - start) * 1000
	size = sys.getsizeof(counts)
	return (this_time, size, counts)

def check_differences(capacity, seq, k):
	bf = pybloomfilter.BloomFilter(capacity, 0.01, None)
	bf2 = pybloomfilter.BloomFilter(capacity, 0.01, None)
	counts_one = create_count_table(seq, k, bf)
	counts_two = second_count_table(seq, k, bf2)
	kmerCount = 0
	diffCount = 0
	kmer_set = set()
	for read in seq:
		kmers = make_kmers(read, k)
		for kmer in kmers:
			if kmer not in kmer_set:
				kmer_set.add(kmer)
				kmerCount = kmerCount + 1
				first = get_count(bf, counts_one, kmer)
				second = get_count(bf2, counts_two, kmer)
				if first != second:
					diffCount = diffCount + 1
	return (kmerCount, diffCount)

def print_timing_size(seq):
	for i in range(1, 6):
		kmerLengths = [20, 40, 50]
		sys.stdout.write("For factor " + str(i) + '\n')
		for k in kmerLengths:
			first = first_implementation_results(seq, len(seq), k)
			second = second_implementation_results(seq, len(seq), k)
			sys.stdout.write("first implementation,  k length = " + str(k) +": " + str(first[0]) + " " + str(first[1]) + '\n')
			sys.stdout.write("second implementation, k length = " + str(k) +": " + str(second[0]) + " " + str(second[1]) + '\n')

def print_differences(seq):
	for i in range(1, 6):
		kmer_lengths = [20, 40, 50]
		sys.stdout.write("For factor " + str(i) + '\n')
		for k in kmer_lengths:
			diff = check_differences(i * len(seq), seq, k)
			sys.stdout.write("for kmer length " + str(k) + ": " + str(diff[1]) + " " +  str(diff[0]) + '\n')

seq, q = readFastq('small_sample.fastq')
print_timing_size(seq)
print_differences(seq)








