import json
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.probability import FreqDist
from nltk import trigrams
from nltk import bigrams

#Part A
my_data=[]
with open("signal-news1.jsonl","r")as f:
    for lines in f:
        my_data.append(json.loads(lines))

for i in my_data:
    my_data[my_data.index(i)]=i['content'].lower()
pattern1="(http://[A-Za-z0-9]+(\.[A-Za-z0-9]*)*(/[A-Za-z0-9]*)*)"   #match url
pattern2="\W"   #match non-alphabetic
pattern3=" [A-Za-z] " # 3 and 4 match single character
pattern4="^[A-Za-z] "
pattern5="([^A-Za-z]+[0-9]+)|([0-9]*[^A-Za-z]+)"   #match pure digits

for i in my_data:
    a=re.sub(pattern1,'',i)
    b=re.sub(pattern2,' ',a) #keep white space
    c=re.sub(pattern3,' ',b)
    d=re.sub(pattern4,'',c)
    e=re.sub(pattern5,' ',d)
    my_data[my_data.index(i)]=nltk.word_tokenize(e)

lemmatizer = WordNetLemmatizer()

'''
from nltk.corpus import wordnet
def get_word_pos(pos_tag):
    if pos_tag.startswith('J'):
        return wordnet.ADJ
    elif pos_tag.startswith('V'):
        return wordnet.VERB
    elif pos_tag.startswith('N'):
        return wordnet.NOUN
    elif pos_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''

for news in my_data:
    news_pos=nltk.pos_tag(news)
    for i in range(len(news)):
        word_pos=get_word_pos(news_pos[i][1])
        if word_pos!='':
            news[i]=lemmatizer.lemmatize(news[i],pos=word_pos)
'''

for news in my_data:
    for i in range(len(news)):
        news[i]=lemmatizer.lemmatize(news[i])

# Part B

corpus=[]
for i in my_data:
    corpus.extend(i)

N=len(corpus)
vocab=set(corpus)
V=len(vocab)

print(N,V)

tri=trigrams(corpus)
f=FreqDist(tri)
top_tri=f.most_common(25)
top_tri=[i[0] for i in top_tri]
print(top_tri)

positive_words = []
negative_words = []
with open("opinion-lexicon-English/positive-words.txt", 'r') as f:
    positive_words = f.read().splitlines()

with open("opinion-lexicon-English/negative-words.txt", 'r') as f:
    negative_words = f.read().splitlines()


    def binary_search(lis, word):
        left = 0
        right = len(lis) - 1
        while left <= right:
            mid = (left + right) // 2
            if word < lis[mid]:
                right = mid - 1
            elif word > lis[mid]:
                left = mid + 1
            else:
                return mid
        return -1

pos=0
neg=0
for i in corpus:
    w1=binary_search(positive_words,i)
    if w1!=-1:
        pos+=1
    w2=binary_search(negative_words,i)
    if w2!=-1:
        neg+=1
print(pos,neg)

pos_news = 0
neg_news = 0
for news in my_data:
    pos = 0
    neg = 0

    for i in news:
        w1 = binary_search(positive_words, i)
        if w1 != -1:
            pos += 1
        w2 = binary_search(negative_words, i)
        if w2 != -1:
            neg += 1
    if pos > neg:
        pos_news += 1
    elif pos < neg:
        neg_news += 1

print(pos_news, neg_news)

# Part C

cor1=[]
for i in my_data[:16000]:
    cor1.extend(i)
vocab1=set(cor1)

tri_1=trigrams(cor1)
ft1=FreqDist(tri_1)
count_t=[i for i in ft1.values()]
count_t=list(map(lambda x :x+1,count_t))
trigram1=[i for i in ft1.keys()]
tri1=dict(zip(trigram1,count_t))

bi_1=[i for i in bigrams(cor1)]
fb1=FreqDist(bi_1)
V2=len(set(bi_1))
count_b=[i for i in fb1.values()]
count_b=list(map(lambda x :x+V2,count_b))
bigram1=[i for i in fb1.keys()]
bi1=dict(zip(bigram1,count_b))

import math
def prob(w1,w2,w3):
    if (w1,w2,w3) in tri1 and (w1,w2) in bi1:
        p=tri1[(w1,w2,w3)]/bi1[(w1,w2)]
        log_p=math.log(p)
        return log_p

w1='is'
w2='this'
sen=['is','this']
for i in range(8):
    score=[]
    words=[]
    for v in vocab1:
        log_p=prob(w1,w2,v)
        if log_p !=None:
            score.append(round(log_p,5))
            words.append(v)
    w3=words[score.index(max(score))]
    w1=w2
    w2=w3
    print(w3)
    sen.append(w3)

sentence=' '.join(sen)
print(sentence)
'''
cor2=[]
for i in my_data[16000:]:
    cor2.extend(i)
vocab2=set(cor2)

tri2=[i for i in bigrams(cor2)]
ft2=FreqDist(tri2) # the dict to store (trigram-number of trigram) pairs
count_t2=[i for i in ft2.values()]
count_t2=list(map(lambda x :x+1,count_t2))
trigram2=[i for i in ft2.keys()]
tri2=dict(zip(trigram2,count_t2))

bi2=[i for i in bigrams(cor2)]
fb2=FreqDist(bi2)
V22=len(set(bi2))
count_b2=[i for i in fb2.values()]
count_b2=list(map(lambda x :x+V22,count_b2))
bigram2=[i for i in fb2.keys()]
bi2=dict(zip(bigram2,count_b2))

def prob2(w1,w2,w3):
    p=tri2[(w1,w2,w3)]/bi2[(w1,w2)]
    log_p=math.log(p)
    return log_p

pp=[]
for news in my_data[16000:]:
    t=trigrams(news)
    b=bigrams(news)
    w1=news[0]
    w2=news[1]
    total_log=[]
    for i in news[2:]:
        log_p=prob2(w1,w2,i)
        if log_p!=None:
            total_log.append(log_p)
        w1=w2
        w2=i
    sum_t=sum(total_log)
    print(sum_t)
    perplexity=pow(1/abs(sum_t),1/len(news))
    pp.append(perplexity)

perp=sum(pp)/len(pp)
print(perp)
'''