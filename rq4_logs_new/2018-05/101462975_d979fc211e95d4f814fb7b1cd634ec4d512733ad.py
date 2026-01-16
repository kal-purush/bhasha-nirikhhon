import nltk.classify.util
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import PlaintextCorpusReader, stopwords
from nltk.classify import accuracy as nltk_accuracy
import random
import re
import string as s


spam = 'spam_data.csv'
ham = 'ham_data.csv'

def rmstopwords(string):
    tmp = string.split(' ')
    for x in tmp:
        if x in stopwords.words('English'):
            tmp.remove(x)
    return ' '.join(tmp)

def rmpunc(string):
    tmp = string.split(' ')
    shit = []
    for x in tmp:
        mt = x.maketrans(s.punctuation, len(s.punctuation) * ' ')
        x = x.translate(mt)
        x = re.sub("\s*", '', x)
        shit.append(x)
    return ' '.join(shit)

message_corpus = PlaintextCorpusReader('./',[spam, ham])
all_message = message_corpus.words()

def massage_feature(word):
    return {'feature':word}

labels_name = ([(massage,'spam') for massage in message_corpus.words(spam)]+[(massage,'ham') for massage in message_corpus.words(ham)])
random.seed(7)
random.shuffle(labels_name)

featuresets = [(massage_feature(n),massage) for (n,massage) in labels_name]
train_set,test_set = featuresets[1500:],featuresets[:1500] #分割数据集
classifier = NaiveBayesClassifier.train(train_set)
features = [y for (x, y) in classifier.most_informative_features()]

spam_word_pro = dict(
    zip([x for x in features], [classifier.prob_classify({'feature': x}).prob('spam') for x in features]))
ham_word_pro = dict(
    zip([x for x in features], [classifier.prob_classify({'feature': x}).prob('ham') for x in features]))

print('训练结果准确率：', str(100*nltk_accuracy(classifier,test_set))+str('%'))

def predict(data):
    email_spam_prob = 0.0

    data = rmstopwords(data)
    data = rmpunc(data)
    words = data.split(' ')
    spam_prob = 0.5  # 假设P(spam) = 0.5
    ham_prob = 0.5  # P(ham) = 0.5
    prob_dict = {}
    for word in words:  # 统计测试集所出现单词word的P(spam|word)
        Psw = 0.0
        if word not in spam_word_pro:
            Psw = 0.4  # 第一次出现的新单词设P(spam|new word) = 0.4 by Paul Graham
        else:
            Pws = spam_word_pro[word]  # P(word|spam)
            Pwh = ham_word_pro[word]  # P(word|ham)
            Psw = spam_prob*(Pws/(Pwh*ham_prob+Pws*spam_prob))
            # P(spam|word) = P(spam)*P(word|spam)/P(word)
            #              = P(spam)*P(word|spam)/(P(word|ham)*P(ham)+P(word|spam)*P(spam))
        prob_dict[word] = Psw
    numerator = 1
    denominator_h = 1
    for k, v in prob_dict.items():
        numerator *= v  # P1P2…Pn = P(spam|word1)*P(spam|word2)*…*P(spam|wordn)
        denominator_h *= (1-v)  # (1-P1)(1-P2)…(1-Pn) = (1-P(spam|word1))*(1-P(spam|word2))*…*(1-P(spam|wordn))
    email_spam_prob = round(numerator/(numerator+denominator_h), 4)
    # P(spam|word1word2…wordn) = P1P2…Pn/(P1P2…Pn+(1-P1)(1-P2)…(1-Pn))

    if email_spam_prob > 0.25:  # P(spam|word1word2…wordn) > 0.25 认为是spam垃圾邮件
        # print('spam')
        # print(email_spam_prob)
        return 1
    else:
        #print('ham')
        return 0