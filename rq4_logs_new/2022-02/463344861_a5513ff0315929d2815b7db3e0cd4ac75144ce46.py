#imports
from lib2to3.pgen2.tokenize import tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
import emoji 
import re
import string
import en_core_web_sm


# adding wsb/reddit flavour to vader to improve sentiment analysis, score: 4.0 to -4.0
customWordRatings = {
    'citron': -4.0,  
    'hidenburg': -4.0,        
    'moon': 4.0,
    'highs': 2.0,
    'mooning': 4.0,
    'long': 2.0,
    'short': -2.0,
    'call': 4.0,
    'calls': 4.0,    
    'put': -4.0,
    'puts': -4.0,    
    'break': 2.0,
    'tendie': 2.0,
     'tendies': 2.0,
     'town': 2.0,     
     'overvalued': -3.0,
     'undervalued': 3.0,
     'buy': 4.0,
     'sell': -4.0,
     'gone': -1.0,
     'gtfo': -1.7,
     'paper': -1.7,
     'bullish': 3.7,
     'bearish': -3.7,
     'bagholder': -1.7,
     'stonk': 1.9,
     'green': 1.9,
     'money': 1.2,
     'print': 2.2,
     'rocket': 2.2,
     'bull': 4.0,
     'bear': -4.0,
     'pumping': -1.0,
     'sus': -3.0,
     'offering': -2.3,
     'rip': -4.0,
     'downgrade': -3.0,
     'upgrade': 3.0,     
     'maintain': 1.0,          
     'pump': 1.9,
     'hot': 1.5,
     'drop': -2.5,
     'rebound': 1.5,  
     'crack': 2.5,
    'gang': 2.0,
     'scam': -2.0,
    'chamath': -2.0,
     'snake': -2.0,
    'squezze': 3.0,
     'bag': -4.0,
     'fly': 2.0,     
     'way': 2.0,     
     'high': 2.0,
     'volume': 2.5,
     'low': -2.0,
     'trending': 3.0,
     'upwards': 3.0,
     'prediction': 1.0,     
     'cult': -1.0,     
    'big': 2.0,}


def removeEmoji(text: str):
    return emoji.get_emoji_regexp().sub(r'', text)

def removePunctuation(text: str):
    text  = "".join([char for char in text if char not in string.punctuation])
    return re.sub('[0-9]+', '', text) 

def tokenizeText(text: str):
    tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|http\S+')
    tokenized_string = tokenizer.tokenize(text)
    return  [word.lower() for word in tokenized_string]

def removeStopWord(tokenizedComment):
    nlp = en_core_web_sm.load()
    stopwords = nlp.Defaults.stop_words
    return [word for word in tokenizedComment if not word in stopwords]

def lematization(commentList):
    lemmatizer = WordNetLemmatizer()
    return ([lemmatizer.lemmatize(w) for w in commentList])

def stockTextSentimentAnalysis(topStocks, stockTexts):
    
    scores = {}
    
    vader = SentimentIntensityAnalyzer()
    
    # adding custom words from data.py 
    vader.lexicon.update(customWordRatings)
    
    for stock in topStocks:
        stockComments = stockTexts[stock]
        for comment in stockComments:
            
            comment = removeEmoji(comment)
            comment = removePunctuation(comment)
            tokenizedComment = tokenizeText(comment)
            commentList = removeStopWord(tokenizedComment)
            lemmatizedComment = lematization(commentList)

            commentScore = {'neg': 0.0, 'neu': 0.0, 'pos': 0.0, 'compound': 0.0}
            
            word_count = 0
            for word in lemmatizedComment:
                score = vader.polarity_scores(word)
                word_count += 1
                for key, _ in score.items():
                    commentScore[key] += score[key] 
            
            try: 
                for key in commentScore:
                    commentScore[key] = commentScore[key] / word_count
            except: pass
            
            if stock in scores:
                for key, _ in commentScore.items():
                    scores[stock][key] += commentScore[key]
            else:
                scores[stock] = commentScore
        
        for key in commentScore:
            #scores[stock][key] = scores[stock][key] / symbols[symbol]
            scores[stock][key]  = "{pol:.3f}".format(pol=scores[stock][key])
    
    return scores