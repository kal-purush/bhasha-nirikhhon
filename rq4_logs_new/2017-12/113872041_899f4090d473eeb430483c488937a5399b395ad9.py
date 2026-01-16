#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 16:24:35 2017

@author: jenkinsc11
"""

def computeLCS(master, subseq):
    """
	Wrapper method to compute the Longest Common Subsequence in an inputed master string given an inputed proposed subsequence string.
	Note that the length of the 'subseq' string parameter must be less than or equal to the length of the 'master' string parameter.
	
	This dynamic programming algorithm runs in O(n^2) time where n is the length of the 'master' string parameter.
	The total space required is O(n^2) due to the memoization step.
    """

    memoized = LCSLength(master, subseq)
    return "".join(LCSBackTrack(subseq, memoized))    


def LCSLength(master, subseq):
    """
	Returns a multi-dimensional list that contains the length of the Longest Common Subsequence (LCS).
	Note that the length of the 'subseq' string parameter must be less than or equal to the length of the 'master' string parameter.
    """       

    if len(subseq) > len(master):
       raise Exception("The length of the subsequence must be less than or equal to the length of the master string being tested")
    # Build a multi-dimensional list filled with 0s based on the inputed parameters
    memoized = [[0]*(min(len(master), len(subseq))+1) for i in range(max(len(master), len(subseq))+1)]        	

    # Populate the multi-dimensional list with the length of the longest common subsequence
    for masterIndex in range(len(master)):
        for subseqIndex in range(len(subseq)):	    
            if master[masterIndex] == subseq[subseqIndex]:
                memoized[masterIndex+1][subseqIndex+1] = memoized[masterIndex][subseqIndex] + 1
            else:
                memoized[masterIndex+1][subseqIndex+1] = max(memoized[masterIndex+1][subseqIndex], memoized[masterIndex][subseqIndex+1])

    return memoized

def LCSBackTrack(subseq, memoized):
    """
	Uses the multi-dimensional list containing the memoized LCS length values to construct a list containing the characters that make up 
	the LCS.	
    """

    lcs = []
    # Get the value in the bottom right hand corner of the inputed multi-dimensional list
    height = len(memoized)-1
    width = len(memoized[height])-1

    while height >= 0 and width >= 0 and memoized[height][width] != 0:
        if memoized[height][width-1] != memoized[height][width]:
            if memoized[height-1][width] != memoized[height][width]:
                lcs.append(subseq[width-1])
            height -= 1
            continue
        width -= 1
        continue   
	
    lcs.reverse()
    return lcs
		      

if __name__ == "__main__":
   master = "BANANANT"
   subseq = "ANTO"
   print (computeLCS(master, subseq))

filename = 'aa010.fasta'

def read_FASTA_strings(filename):
    with open(filename) as file:
        return file.read().split('>')[1:]

def read_FASTA_entries(filename):
    return [seq.partition('\n') for seq in read_FASTA_strings(filename)]
    
def read_FASTA_sequences(filename):
    return [[seq[0], seq[2].replace('\n', '')]           
             for seq in read_FASTA_entries(filename)]

aa_sequence = list(seq[1] for seq in read_FASTA_sequences(filename))

aa_sequence.sort(key = (len), reverse = True)

sequence_dictionary = {}
   

for seq in sorted(aa_sequence):
   sequence_dictionary[aa_sequence.index(seq)] = seq

for value in sequence_dictionary:
    if value is not (len(sequence_dictionary) -1) and (value + 2) <= (len(sequence_dictionary) -1):
        print('Longest Common Subsequence between sequences:', value, (value + 1), computeLCS(sequence_dictionary[value], sequence_dictionary[value + 1]), '\n')
        print('Longest Common Subsequence between sequences:', value, (value + 2), computeLCS(sequence_dictionary[value], sequence_dictionary[value + 2]), '\n')
  