""" This is a solution to an exercise from

Think Python, 2nd Edition
by Allen Downey
http://thinkpython2.com

Copyright 2015 Allen Downey

License: http://creativecommons.org/licenses/by/4.0/

Exercise 13_2: 
Modify your program from the previous exercise to read the book you downloaded,
skip over the header information at the beginning of the file, and process the rest of
the words as before.

Then modify the program to count the total number of words in the book, and the
number of times each word is used.

Print the number of different words used in the book. Compare different books by
different authors, written in different eras. Which author uses the most extensive
vocabulary?
"""

from exercise13_1 import file_to_words


def text_histogram(filename):
	"""Takes a string representing a filename as input and returns
	a dictionary histogram of the words in the file"""

	hist = dict()

	for line in file_to_words(filename):
		hist[line] = hist.get(line, 0) + 1

	return hist

def vocab_size(filename):
	"""Takes a text file as input and outputs the number of different 
	words in the text"""
	hist = text_histogram(filename)
	vocab_count = 0
	for line in hist:
		vocab_count += 1
	return vocab_count

if __name__ == "__main__":
	print(text_histogram("sciencefiction.txt"))
	print(vocab_size("sciencefiction.txt"))