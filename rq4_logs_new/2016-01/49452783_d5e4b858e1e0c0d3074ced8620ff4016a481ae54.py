# put your code here.
#open txt file
#read file line by line
#create empty dict
# for every new code create a new key w val 1
# for repeat words increment value

def count_words(file_name):
    """Counts words in a txt file"""

    word_log = open(file_name)

    word_occurance = {}

    for line in word_log:
        line = line.rstrip()
        words = line.split()

        for word in words:
            quantity = word_occurance.get(word, 0)
            word_occurance[word] = quantity + 1    

    print word_occurance


count_words("test.txt")    