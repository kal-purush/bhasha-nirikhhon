import nltk
from nltk.corpus import stopwords
from nltk.stem.lancaster import LancasterStemmer
import pandas

# nltk.download('punkt')

stemmer = LancasterStemmer()

training_data = [{"class": "greeting", "sentence": "как дела?"},
                 {"class": "greeting", "sentence": "как прошел твой день?"},
                 {"class": "greeting", "sentence": "добрый день"},
                 {"class": "greeting", "sentence": "что делаешь сегодня?"},
                 {"class": "goodbye", "sentence": "удачного дня"},
                 {"class": "goodbye", "sentence": "еще увидимся"},
                 {"class": "goodbye", "sentence": "удачного вечера"},
                 {"class": "goodbye", "sentence": "увидемся позже"},
                 {"class": "lunch", "sentence": "сделай мне сэндвич"},
                 {"class": "lunch", "sentence": "можешь сделать мне сэндвич?"},
                 {"class": "lunch", "sentence": "сегодня сэндвич?"},
                 {"class": "lunch", "sentence": "что на обед?"}]

# data = training_data
training_data = training_data

# capture unique stemmed words in the training corpus
corpus_words = {}
class_words = {}
# turn a list into a set (of unique items) and then a list again (this removes duplicates)
classes = list(set([a['class'] for a in training_data]))
for c in classes:
    # prepare a list of words within each class
    class_words[c] = []

# loop through each sentence in our training data
for data in training_data:
    # tokenize each sentence into words
    for word in nltk.word_tokenize(data['sentence']):
        # ignore a some things
        if word not in ["?", "'s"]:
            # stem and lowercase each word
            stemmed_word = stemmer.stem(word.lower())
            # have we not seen this word already?
            if stemmed_word not in corpus_words:
                corpus_words[stemmed_word] = 1
            else:
                corpus_words[stemmed_word] += 1

            # add the word to our words in class list
            class_words[data['class']].extend([stemmed_word])


# calculate a score for a given class taking into account word commonality
def calculate_class_score_commonality(sentence, class_name, show_details=True):
    score = 0
    # tokenize each word in our new sentence
    for word in nltk.word_tokenize(sentence):
        # check to see if the stem of the word is in any of our classes
        if stemmer.stem(word.lower()) in class_words[class_name]:
            # treat each word with relative weight
            score += (1 / corpus_words[stemmer.stem(word.lower())])

            if show_details:
                print("   match: %s (%s)" % (stemmer.stem(word.lower()), 1 / corpus_words[stemmer.stem(word.lower())]))
    return score

# return the class with highest score for sentence
def classify(sentence):
    high_class = None
    high_score = 0
    # loop through our classes
    for c in class_words.keys():
        # calculate score of sentence for each class
        score = calculate_class_score_commonality(sentence, c, show_details=False)
        # keep track of highest score
        if score > high_score:
            high_class = c
            high_score = score

    return high_class, high_score


print(classify(sentence='что сегодня на обед?'))