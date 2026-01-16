"""Word counter

Program that, given a text file, counts the number of words in the file
and reports the top N words ordered by the number of times they appear in the
file.

Author: Arran Cardnell
Date: 08Sep15

"""

from collections import Counter

class WordCounter:
    """A word counting program."""

    def open_file(self):
        """Select a file and check it is valid."""
        
        filename = input("\nEnter the name of the text file you would like to count." \
                     + "\nPlease include the suffice (i.e. '.txt')." + "\n> ")
        try:    
            with open(filename) as f:
                words = [word.strip() for line in f for word in line.split()]  # create a list of all the words in the file
            if len(words) <= 1:
                plural1, plural2 = "is", ""
            else:
                plural1, plural2 = "are", "s"
            print("\nThere {} a total of {} word{} in this file.\n".format(plural1, len(words), plural2))
            return words
        except IOError as ioerr:  # if the filename does not exist, return an error and allow user to choose again
            print("\nFile error: " + str(ioerr))
            return self.open_file()
        
    def topN(self):
        """Allow the user to select a number and check it is valid."""
        
        try:
            return int(input("Enter the number of results you would like" \
                              + " returned." + "\n> "))
        except ValueError:
            print("\nThat's not a number. Please try again.\n")
            return self.topN()

    def counter(self):
        """Counts the words in the chosen file, orders them by their occurrence
           and returns the top number of words specified by the user."""
        
        counted_words = Counter(self.open_file())  # counts and orders words in text file
        topN = self.topN()
        if topN == 1:
            print("\nThe top word is:")        
        elif topN > 1:
            print("\nThe top {} words are:".format(topN))
       
        for word, count in counted_words.most_common(topN):  # returns the top N number of words
            if count <= 1:
                plural_word = ""
            else:
                plural_word = "s"
            print("\n'{}', which appears {} time{}.".format(word, count, plural_word))

if __name__ == "__main__":
    count = WordCounter()
    count.counter()