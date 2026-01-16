# This program takes from stdin, prints any line that contains only characters 
# in the Fula orthography (plus numerals and punctuation) to stdout, and
# prints undesired characters with their number of appearances sorted from 
# highest to lowest to stderr

import sys
lines = sys.stdin.readlines()

import re

import unicodedata

import operator  # Used for sorting our character dictionary by maximum value

nonAlphCount = {}
weirdSpaces = {}

for line in lines:
    for char in line:
          if unicodedata.category(char) == 'Zs' and char != ' ' and char != '\n' and char != '\t':
               weirdSpaces[ord(char)]=weirdSpaces.setdefault(ord(char), 0)+1
               line.replace(char, ' ')
    if re.search(r"[^A-Za-zƁɓƊɗŊŋÑñƳƴ,;?!“”″\.\'\"\(\[\]\)\s\d]", line):  # Checks for any line that contains an undesired character (non alphabet/numeral/punctiation)
         for char in line:
              if re.match(r"[^A-Za-zƁɓƊɗŊŋÑñƳƴ,;?!“”″\.\'\"\(\[\]\)\s\d]", char):  # Finds the undesired character in line that has one
                   nonAlphCount[char]=nonAlphCount.setdefault(char, 0)+1  # Increments the count for this character (or creates an entry with a count of 1)
    else:
         sys.stdout.write(line)  # Prints any line that contains no undesired characters to stdout

sortedNonAlphCount = dict(sorted(nonAlphCount.items(),        # Sorts our dictionary by maximum value, which is the count of each character
                                 key=operator.itemgetter(1), 
                                 reverse = True))

for key, value in sortedNonAlphCount.items():
    sys.stderr.write(str(value) + "   " + key + "\n")  # Prints our entries value first to stderr

sortedWeirdSpaces = dict(sorted(weirdSpaces.items(),        # Sorts our dictionary by maximum value, which is the count of each character
                                 key=operator.itemgetter(1), 
                                 reverse = True))

for key, value in sortedWeirdSpaces.items():
    sys.stderr.write(str(value) + "   " + str(unicodedata.name(chr(key))) + "\n")  # Prints our entries value first to stderr