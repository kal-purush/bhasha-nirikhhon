#!/usr/bin/env python
def count_nucleotides(dna):
    from collections import Counter
    count_nucl = Counter(dna)
    return count_nucl

def main():
    try:
        with open('../data/dataset.txt') as dataset:
            dna = dataset.read().strip()

        # Call helper and count each nucleotide in 
        count_nucl = count_nucleotides(dna)

        # Parse and print answer
        print(' '.join([count_nucl[nucl] for nucl in 'ACGT']))
    except:
        print('[ERR] No dataset found')

if __name__ == '__main__':
    main()