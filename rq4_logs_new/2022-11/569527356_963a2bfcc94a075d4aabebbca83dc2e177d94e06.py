# -*- coding: utf-8 -*-
# Ref：
# https://www.alibabacloud.com/blog/natural-language-processing-in-python-3-using-nltk_595031 
# https://www.nltk.org/book/ch05.html 
# https://github.com/nltk/nltk_data 

# HZAU NLP and BioNLP课程Tutorial
#pip install nltk



import nltk

# 长段落文本准备
longtext = "They reached King's Cross at half past ten. Uncle Vernon dumped Harry's trunk onto a cart and wheeled it into the station for him. Harry thought this was strangely kind until Uncle Vernon stopped dead, facing the platforms with a nasty grin on his face. 'Well, there you are, boy. Platform nine -- platform ten. Your platform should be somewhere in the middle, but they don't seem to have built it yet, do they?' "
print("\n*********The orinigal long text input*********")
print(longtext)


# 长段落文本的分句
from nltk.tokenize import sent_tokenize
print("\n*********Sentence tokenization*********")
print(sent_tokenize(longtext))


#长段落文本的分词
from nltk.tokenize import word_tokenize
token_longtext=word_tokenize(longtext)
print("\n*********Word tokenization*********")
print(token_longtext)


#长段落文本的拆分，与分词进行结果对比
tmp_text=longtext.split()
print("\n*********Word split*********")
print(tmp_text)


#长段落文本的Stemming
from nltk.stem import PorterStemmer
stemmer = PorterStemmer()
print("\n*********Porter Stemmer*********")
for each in token_longtext:
    print(stemmer.stem(each))



#短段落文本的POS tagging
shorttext = "They reached King's Cross at half past ten."
token_shorttext = word_tokenize(shorttext)
tag = nltk.pos_tag(token_shorttext)
print("\n*********Print POS tagging*********")
print(tag)


#短段落文本的词频计算
fd_token_shorttext = nltk.FreqDist(token_shorttext)
print("\n*********Word count and word count visualization*********")
print([fd_token_shorttext])
#fd_token_shorttext.plot()



#短段落文本的Brown语料库相似词汇挑选
print("\n*********Similar words in Brown corpus*********")
browntext = nltk.Text(word.lower() for word in nltk.corpus.brown.words())
for each in token_shorttext:
    print("\nThe similar words for \"{}\" in Brown include:".format(each))
    browntext.similar(each)


#短段落文本的WordNet语料相似词汇挑选
from nltk.corpus import wordnet
for each in token_shorttext:
    print("\nThe similar words for \"{}\" in WordNet include:".format(each))
    syn = wordnet.synsets(each)
    synonyms = []
    for syn in wordnet.synsets(each):
        for lemma in syn.lemmas():
            synonyms.append(lemma.name())
    print(synonyms)