__author__ = 'Alec'

"""
Given: A string s of length at most 10000 letters.

Return: How many times any word occurred in string. Each letter case (upper or lower) in word matters. Lines in output can be in any order.

Sample Dataset:

We tried list and we tried dicts also we tried Zen

Sample Output:

and 1
We 1
tried 3
dicts 1
list 1
we 2
also 1
Zen 1

"""
# obtain string
#s = raw_input("String? ")

file = open("rosalind_ini6.txt",'r')

s = file.read().strip()

file.close()

# create empty dictionary/map
word_count = {}

# iterate through words in string
for word in s.split(" "):
    # if the word exists as a key, increment value
    if(word_count.has_key(word)):
        word_count[word] += 1
    #if the word does not exist, create with initial value of 1
    else:
        word_count[word] = 1

output = open("output1.txt","w")

# print keys and values
for key in word_count:
    output.write(key + " " + str(word_count[key]) + "\n")

output.close()