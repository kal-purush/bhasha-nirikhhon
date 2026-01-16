def review_messages(msg):
    #convert the message to lower case
    msg = msg.lower();
    
    return msg;
    

def remove_stopWords(msg):
    from nltk.corpus import stopwords
    stopwords = stopwords.words('english') 
    
    msg = [word for word in msg.split() if word not in stopwords]
    msg = " ".join(word for word in msg)
    
    return msg;
    
def stem_words(msg):
    from nltk import stem
    
    stemmer = stem.SnowballStemmer('english')
    
    msg = [word for word in msg.split()]
    
    msg = " ".join([stemmer.stem(word) for word in msg])
    
    return msg