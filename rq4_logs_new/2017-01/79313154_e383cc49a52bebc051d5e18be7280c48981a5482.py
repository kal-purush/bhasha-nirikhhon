import nltk
import random
import pyttsx
from nltk.corpus import movie_reviews
from sklearn.naive_bayes import MultinomialNB,BernoulliNB
from nltk.classify.scikitlearn import SklearnClassifier
documents = [(list(movie_reviews.words(fileid)), category)
             for category in movie_reviews.categories()
             for fileid in movie_reviews.fileids(category)]
'''p=pyttsx.init()
x=p.say(documents)
p.runAndWait()'''

random.shuffle(documents)

all_words = []

for w in movie_reviews.words():
    all_words.append(w.lower())

all_words = nltk.FreqDist(all_words)

word_features = list(all_words.keys())[:3000]


def find_features(document):
    words = set(document)
    features = {}
    for w in word_features:
         features[w] = (w in words)
        

    return features
#print((find_features(movie_reviews.words('neg/cv000_29416.txt'))))
featuresets = [(find_features(rev), category) for (rev, category) in documents]
#featuresets=[]
'''for sev,category in documents :
   featuresets.append(find_features(sev),category)
   print sev[:1]
   #featuresets.append(category)
 '''
#print featuresets[:300]
training_set=featuresets[:1900]
test=featuresets[1900:]
classifier=nltk.NaiveBayesClassifier.train(training_set)

MNC=SklearnClassifier(MultinomialNB())
MNC.train(training_set)
print "Accc:", nltk.classify.accuracy(classifier,test)
print classifier.show_most_informative_features(25)
print "Accc:", (nltk.classify.accuracy(MNC,test)*100)
 