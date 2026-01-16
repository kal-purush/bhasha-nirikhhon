#!/usr/bin/env python

def count_gc(dna):
	gc_count = 0
	for base in dna:
		if base == 'G' or base == 'C':
			gc_count += 1
	return round(gc_count / len(dna), 8)

def main():
	gc_dict = {}
	with open('../data/dataset.txt') as dataset:
		names, dna = [], ''
		for line in dataset:
			if line.startswith('>'):
				names.append(line[1:].strip())
				dna += '/'
			else:
				dna += line.strip()

		for i in range(len(names)):
			gc_dict[count_gc(dna[1:].split('/')[i])] = names[i]

	sorted_tups = sorted(list(gc_dict.items()))
	print(sorted_tups[-1][1], 100 * sorted_tups[-1][0], end='%\n')

if __name__ == '__main__':
	main()