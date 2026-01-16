def count_string_word():
    str = 'mareena'
    print (str.count('a',0,len(str)))


def count_words_characters(str):
    """
    str -> number
    Return the number of characters in a string.
    Return the number of strings in a sentence.
    >>> count_words_characters("Hello World")
    >>> Number of words in the string:  2
    >>> Number of characters in the string: 11
    """

    #str = "Hello World"
    char = 0
    word = 1

    for i in str:
        print("Char:", i)
        char = char +1
        if(i ==' '):
            word = word +1
    print("Number of words in the string: ",word)
    print ("Number of characters in the string:",char)

count_words_characters("Hello World")

from collections import Counter
def find_dup_char(input):
    '''
    :param input: string
    :return: char
    Return all duplicate characters from a given string.

    >>> find_dup_char("hello")
    >>> l
    '''
    # Create a dictionary using counter method with strings as keys and their frequencies as values

    dict = Counter(input)
    j = -1
    for i in dict.values():
        j = j + 1
        if (i > 1):
            print (dict.keys(), [j])


find_dup_char("helllo")


def two_strings_anagram(str1,str2):
    """
     Check two strings are anagram
     >>> two_strings_anagram("spar","rasp")
     >>> The strings are anagrams.
     >>> two_strings_anagram("spark","rasp")
     >>> sorted first string :  ['a', 'k', 'p', 'r', 's']
     >>> sorted second string :  ['a', 'p', 'r', 's']
     >>> The strings are not anagrams.
    """
    # use sorted() to sort both the strings into lists.
    s1 = sorted(str1)
    print("sorted first string : ", s1)
    s2  = sorted(str2)
    print("sorted second string : ", s2)
    if (s1 == s2):
        print("The strings are anagrams.")
    else:
        print("The strings are not anagrams.")


two_strings_anagram("spar","rasp")
two_strings_anagram("spark","rasp")
