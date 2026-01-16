
words = open("spell.words").readlines()
words = map(lambda x: x.strip(), words)
# print words
print len(words)
print len(words[0])
print words[len(words)-1]

print('zygotic' in words)
print('mistasdas' in words)

def load_words(file_name) :
	words = open(file_name).readlines()
	words = map(lambda x: x.strip(), words)
	return words

def check_word(words, word) :
		return word in words
		
def check_words(words, sentence):
    words_to_check = sentence.split(' ')
    for word in words_to_check:
        if not check_word(words, word):
            print('Word is misspelt : ' + word)
            return False
    return True

	
		
print load_words("spell.words")[0]
print check_word(words, 'zygotic')
print check_word(words, 'mistasdas')