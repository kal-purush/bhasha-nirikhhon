"""
CP1404 Practical
Do-from-scratch-Exercise
"""

word_string = input("Please enter a sentence")
word_list = word_string.split()
word_freq = []
for w in word_list:
    word_freq.append(word_list.count(w))
word_pair = dict(zip(word_list, word_freq))
print("text: {}".format(word_string))
for word in word_pair:
    print("{} : {}".format(word,word_pair[word]))