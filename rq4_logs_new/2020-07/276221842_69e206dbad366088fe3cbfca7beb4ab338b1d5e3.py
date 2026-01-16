#!/usr/bin/env python

def make_profile_matrix(dnas, n):
	profile = {'A': [0] * n, 'C': [0] * n, 
		'G': [0] * n, 'T': [0] * n}
	for i in range(n):
		for dna in dnas:
			profile[dna[i]][i] += 1
	return profile

def compute_consensus_string(profile, n):
	consensus = ''
	for i in range(n):
		column = []
		for base in profile:
			column += [(profile[base][i], base)]
		consensus += max(column)[1]
	return consensus

def main():
	with open('../data/dataset.txt') as dataset:
		dnas, dna = [], ''
		for line in dataset:
			if line.startswith('>'):
				dna += '/'
			else:
				dna += line.strip()

	dnas = dna[1:].split('/')
	n=len(dnas[0])
	profile = make_profile_matrix(dnas, n=len(dnas[0]))
	consensus = compute_consensus_string(profile, n)

	print(consensus)
	for base in profile:
		print(base, end=': ')
		for num in profile[base]:
			print(num, end=' ')
		print()

if __name__ == '__main__':
	main()