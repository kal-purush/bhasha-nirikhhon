from textblob import TextBlob
import json
import numpy as np

def load_data(n: int) -> list:
    data = []
    with open("gg2020.json") as file:
        for line in file.readlines():
            data.append(json.loads(line[:-1])["text"])
            if len(data) > n: break

    return data

def sentiment(sentences: list) -> tuple:
    '''Can be used on entity's sentiment and red carpet, Humor (find the word dress/joke first)
       best dressed (++), worst dressed (-+), most discussed(.-), most controversial(--),'''
    polarity, subjectivity = 0, 0
    for sentence in sentences:
        tb = TextBlob(sentence.replace("\n", ""))
        polarity += tb.sentiment.polarity
        subjectivity += tb.sentiment.subjectivity
    
    polarity, subjectivity = polarity/len(sentences), subjectivity/len(sentences)

    return polarity, subjectivity

def find_keyword(sentences: list, keyword: str) -> list:
    '''keyword should be in lower case'''
    data = []
    for sentence in sentences:
        tb = TextBlob(sentence)
        if keyword in tb.lower(): data.append(sentence)
    
    return data

data = load_data(138582)

def additional(entities: list, typ: str, data: list = data):
    if typ not in ["dress", "joke", "sentiment", "parties", "act"]: raise Exception

    P, S = [], []

    for entity in entities:
        data = find_keyword(data, entity)

        if typ in ["dress", "joke", "act"]: data = find_keyword(data, typ)
        elif typ in ["parties"]:
            # Find other actor
            pass
        
        if not len(data):
            P.append(0)
            S.append(0.5)
            continue
        
        p, s = sentiment(data)
        P.append(p)
        S.append(s)

    P, S = np.array(P), np.array(S)

    ## Do something to return

additional(["tom hanks", "taylor swift", "jessica henwick"], "dress", data)