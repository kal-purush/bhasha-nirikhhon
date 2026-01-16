#!/usr/bin/env python

def dominant_prob(k, m, n):
	N = k + m + n
	prob_dominant_k = k/N
	prob_dominant_m = m/N * (k/(N-1) + (0.75 * (m-1)/(N-1) + (0.5 * (n)/(N-1))))
	prob_dominant_n = n/N * (k/(N-1) + 0.5 * m/(N-1))
	return prob_dominant_k + prob_dominant_m + prob_dominant_n

def main():
	try:
		with open('../data/dataset.txt') as dataset:
			k, m, n = dataset.readline().strip().split()
			k, m, n = int(k), int(m), int(n)
		prob = dominant_prob(k, m, n)
		print(prob)
	except:
		print('[ERR] No dataset found')

if __name__ == '__main__':
	main()