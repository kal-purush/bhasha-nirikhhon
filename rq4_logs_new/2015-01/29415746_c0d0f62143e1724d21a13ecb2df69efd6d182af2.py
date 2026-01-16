# Bloom filter implementation with false positive probability of 1%

from bitarray import bitarray

P = 127                                     # Block size
k = 7                                       # No of hash functions

filter_set = bitarray(P * k)
filter_set.setall(False)


# Two hash functions : hash1 and hash2

def hash(s, acc):
        global P
        value = 0

        for c in s:
                value = (ord(c) + acc * value) % P

        return value


def hash1(s):
        return hash(s, 31)

def hash2(s):
        return hash(s, 10)

def comp_hash(s, i):
        global P
        return (hash1(s) + i * hash2(s)) % P

def addString(vector, s):
        global P, k

        for i in xrange(k):
                index = comp_hash(s, i)
                vector[P * i + index] = True

def ismember(vector, s):
        global P, k
        for i in xrange(k):
                if (not vector[P * i + comp_hash(s, i)]):
                        return False

        return True

def test(vector, s):
        if (ismember(vector, s)):
                print("May be Yes")
        else:
                print("Definetely No")