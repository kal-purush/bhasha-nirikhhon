import nltk.classify.util
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import names

# Return positive, neutral and negative sentiment scores for a given sentence based on the defined vocabulary

# Feature extraction method
def word_feats(word):
    return dict([(letter, True) for letter in word])

# Define vocabulary
positive_vocab = [ 'awesome', 'outstanding', 'fantastic', 'terrific', 'good', 'nice', 'great', 'super','superb',':)' ]
negative_vocab = [ 'bad', 'terrible','useless', 'hate', 'pathetic','worst', ':(' ]
neutral_vocab = [ 'movie','the','sound','was','is','actors','did','know','words','not' ]

# Classify training set vocabulary
positive_features = [(word_feats(pos), 'pos') for pos in positive_vocab]
negative_features = [(word_feats(neg), 'neg') for neg in negative_vocab]
neutral_features = [(word_feats(neu), 'neu') for neu in neutral_vocab]
 
train_set = negative_features + positive_features + neutral_features

# Train using Naive Bayes Classifier
classifier = NaiveBayesClassifier.train(train_set) 
 
# Predict
neg = 0
pos = 0
neu = 0

query_sentence = input("sentence: ")

query_sentence = query_sentence.lower()
words = query_sentence.split(' ')
for word in words:
    classResult = classifier.classify( word_feats(word))
    if classResult == 'neg':
        neg = neg + 1
    if classResult == 'pos':
        pos = pos + 1
    if classResult == 'neu':
        neu = neu + 1

print('Positive: ' + str(float(pos)/len(words)))
print('Neutral: ' + str(float(neu)/len(words)))
print('Negative: ' + str(float(neg)/len(words)))