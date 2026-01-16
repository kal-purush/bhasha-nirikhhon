from __future__ import division
from bs4 import BeautifulSoup, Comment
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from urllib.parse import urljoin
#import math
import numpy as np
import json
import time
import types
import os


list = dict()
list2= dict()

def checkForStopwords(checkword):
    stopword = set(stopwords.words('english'))
    if checkword in stopword:
        return True
    else:
        return False


def stemWords(stemMe):
    stemOutput = SnowballStemmer('english')
    stemMe = stemOutput.stem(stemMe)
    return stemMe


def processText(links,doc_num):
    for link in links:
        words = link.lower().split()
        for s in words:
            #s = word.encode('utf-8')
            if s.isalnum() and len(s) < 15:
                if not checkForStopwords(s):
                    s = stemWords(s)
                    if s in list:  # adding the word to the dictionary if it is alphanumeric
                        if doc_num in list[s]:
                            list[s][doc_num] += 1  # increment the count if the word already exists
                        else:
                            list[s][doc_num] = 1  # add the word and set count =1 if seen for the first time
                    else:
                        list1={doc_num:1}
                        list[s]=list1
            else:
                p = ""
                for a in s:  # traverse character by character if the word is not alphanumeric
                    if a.isalnum():
                        p = p + a
                    else:
                        if not checkForStopwords(p) and len(p) < 15:
                            p = stemWords(p)
                            if p in list:  # adding the word to the dictionary if it is alphanumeric
                                if doc_num in list[p]:
                                    list[p][doc_num] += 1  # increment the count if the word already exists
                                elif p!="":
                                    list[p][doc_num] = 1  # add the word and set count =1 if seen for the first time
                            else:
                                list1={doc_num:1}
                                list[p]=list1
                            p = ""
                if not checkForStopwords(p) and len(p) < 15:
                    p = stemWords(p)
                    if p in list:  # adding the word to the dictionary if it is alphanumeric
                        if doc_num in list[p]:
                            list[p][doc_num] += 1  # increment the count if the word already exists
                        elif p!="":
                            list[p][doc_num] = 1  # add the word and set count =1 if seen for the first time
                    else:
                        list1={doc_num:1}
                        list[p]=list1




c=0
#fileBasePath = "C:\\Users\\aiswa\\Desktop\\IR\\assign_3\\webpages\\WEBPAGES_RAW\\"
fileBasePath = os.path.abspath("WEBPAGES_RAW")
print(fileBasePath)
print('Indexing: This will take a while')
for i in range(75):
    fName1 = '%d' %i
    if i<=73:
        N = 500
    else:
        N = 497
    for j in range(N):
        fName2 = '%d' %j
        docNum = fName1 + '\\' + fName2
        filePath = fileBasePath + '\\'+ docNum
        filePath=filePath.replace("\\","/")
        with open(filePath,"r", encoding="utf-8") as f:
            duplicate=False
            soup = BeautifulSoup(f.read(),"lxml")


            print (docNum)
            if soup.head is not None:
                try:

                    text = soup.head.findAll('h1')
                    if text is not None:
                        for term in text:
                            if term is not None:
                                visible_text=[term.string]
                                head=processText(visible_text, docNum.replace("\\","/"))
                except:
                    print ("e")

            if soup.body is not None and not duplicate:
                try:
                    text = soup.body.findAll('h1')
                    if text is not None:
                        for term in text:
                            if term is not None:
                                visible_text = [term.string]
                                body = processText(visible_text, docNum.replace("\\","/"))
                except:
                    print ("e")
            if soup.head is not None:
                try:
                    text = soup.head.findAll('title')
                    if text is not None:
                        for term in text:
                            if term is not None:
                                visible_text=[term.string]
                                head=processText(visible_text, docNum.replace("\\","/"))
                except:
                    print ("e")

            if soup.body is not None and not duplicate:
                try:
                    text = soup.body.findAll('title')
                    if text is not None:
                        for term in text:
                            if term is not None:
                                visible_text = [term.string]
                                body = processText(visible_text, docNum.replace("\\","/"))
                except:
                    print ("e")

print (list)
print ("Number of unique words:",len(list))
with open('Report4', 'w') as fl:
    fl.write(json.dumps(list))