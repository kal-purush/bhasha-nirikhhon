#!/usr/bin/env python
'''
# 002 RNA
Transcribing DNA into RNA

A solution to the RNA transcription problem.

URL: http://rosalind.info/problems/rna/
'''

def transcribe(dna):
	# Simply replace the thymine base with uracil
	return dna.replace('T', 'U')

def main():
	try:
	    with open('../data/dataset.txt') as dataset:
	        dna = dataset.read().strip()

	    # Transcribe the DNA into RNA
	    rna = transcribe(dna).strip()

	    # Print the RNA
	    print(rna)
	except:
		print('[ERR] No dataset found.')

if __name__ == '__main__':
    main()