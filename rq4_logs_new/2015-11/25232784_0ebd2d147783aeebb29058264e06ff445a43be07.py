#!/usr/bin/python
from collections import defaultdict


def anagram(str1, str2):
    if str1 is None or str2 is None or \
            len(str1) != len(str2):
        return False
    hash = defaultdict(int)
    for char in str1:
        hash[char.lower()] += 1
    for char in str2:
        hash[char.lower()] -= 1
        if hash[char.lower()] < 0:
            return False
    return True


def Main():
    if anagram("nitin", "tinin"):
        print "Anagram"
    else:
        print "Not an Anagram"


if __name__ == '__main__':
    Main()