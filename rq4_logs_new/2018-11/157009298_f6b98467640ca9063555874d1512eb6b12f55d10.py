# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 20:21:37 2016

@author: govind
"""
#import necessary corpuses and functions
import nltk
from nltk import word_tokenize
from nltk.corpus import sentiwordnet as swn
wordnet_words = set(w.lower() for w in nltk.corpus.words.words())
from nltk.stem import WordNetLemmatizer
wordnet_lemmatizer = WordNetLemmatizer()
import re
import numpy as np
###Function to strip punctuations
from string import punctuation
def strip_punctuation(s):
    return(''.join([c  for c in s if c not in punctuation]))

###Legal part of speech tags
pos_tags = {'JJ': 'a',
       'JJR': 'a',
       'JJS': 'a',
       'NN': 'n',
       'NNS': 'n',
       'NNP': 'n',
       'NNPS': 'n',
       'RB': 'r',
       'RBR': 'r',
       'RBS': 'r',
       'VB': 'v',
       'VBD': 'v',
       'VBG': 'v',#verb,is being used adjectively or is a gerund(noun)
       'VBN': 'a', #can be a past tense verb or adjective -ambiguous/'a' seems to work more often
       'VBP': 'v',
       'VBZ': 'v',
       }

#List of negation words
negations = ['not','cant','cannot','wont','doesnt','dont','wouldnt','shouldnt',
             'wasnt','couldnt','isnt']

def get_previous_word(word,text):
    i = text.index(word)
    return(text[i-1])
        

    
##Function to extract all sent features
def get_features(tweet):
    '''
    Extracts 24 fsentiment features
    '''
    #Get number of exclamations in a tweet
    exclm_flag = int(len(re.findall('!',tweet)) >0 )  
    #Number of # tags
    noofhashtags = len(re.findall('#',tweet))    
    #Strip punctuations
    tweet = strip_punctuation(tweet)
    #Split tweet into words    
    words = tweet.split()
    #Number of words
    no_of_words = len(words)
    #Get number of capitalized words
    capwords = len([x for x in words if x[0].isupper()])
    #Get number of URLs
    NoofURL = len([x for x in words if x == "URL"])
    #Get number of words entirely in caps
    allcapwords = len([x for x in words if x.isupper()])
    #tweet length
    tweet_len = len(tweet)
    #Get percentage of capitalized letters
    cap_letters = len([x for x in tweet if x.isupper()])
    cap_letter_perc = cap_letters/tweet_len
    #Lemmatize tweets before calculating polarity    

    tweet = ' '.join([wordnet_lemmatizer.lemmatize(x) for x in words])
    
    #Tokenize for senti-features
    text = tweet.split()
    
    #No of negation words
    negation_words = len([x for x in text if x in negations])
    #parts of speech that have a sentiment
    pos1 = nltk.pos_tag(text)
    pos2 = [pos_tags.get(x[1]) for x in pos1] # Get POS for all words in terms of n,r,a,v
    #Number of nouns
    Noofnouns = pos2.count('n')    
    #Number of verbs
    Noofverbs = pos2.count('v')
    #Number of adjectives
    Noofadj = pos2.count('a')
    #Number of adverbs
    Noofadv = pos2.count('r')    
    
    legal_pos = [x for x in pos1 if x[1] in pos_tags.keys() ]
    ##Check if word is in dictionary
    legal_words =[x for x in legal_pos if x[0].lower() in wordnet_words]
     #Input into sentiment function
    sent_input = ['.'.join([x[0],pos_tags.get(x[1],'NA'),'01'])  for x in legal_words]
    #Get sentiment
    sentiment = []
    illegal_words =[]
    for x in sent_input:
        
        pos = ['r','n','v','a'] 
        pieces = x.split('.')
        try:
            pos.remove(pieces[1])
            sentiment.append(swn.senti_synset(x))

        except:
           try:
               pos1 = pos.pop(0)
               sentiment.append(swn.senti_synset('.'.join([pieces[0],pos1,pieces[2]])))
           except:
               try:
                   pos1 = pos.pop(0)        
                   sentiment.append(swn.senti_synset('.'.join([pieces[0],pos1,pieces[2]])))
               except:
                   try:
                       sentiment.append(swn.senti_synset('.'.join([pieces[0],pos[0],pieces[2]])))
                   except:
                       illegal_words.append(pieces[0])
    #Get scores
    scores =  [[x.pos_score(),-x.neg_score()] for x in sentiment]   
    #Get word - pos combination
    legal_words2 = [(x[0],pos_tags.get(x[1])) for x in legal_words  if x[0] not in illegal_words]   
     #Get scores
    score_dict = dict(zip(legal_words2,scores))
    
    ######Flip polarity if previous word was a negation word    

    #Identufy words which have a negation prior to it
    negated_words = [x for x in score_dict if get_previous_word(x[0],text) in negations]
    for word in negated_words:
        score_dict[word] = [-score_dict[word][1],-score_dict[word][0]]
    
    #words with a prior polarity    
    polar_words = [x for (x,y) in score_dict if  (score_dict[(x,y)][0]>0 or score_dict[(x,y)][1]<0)]
    #no of words with a polarity    
    no_polar_words = len(polar_words)
    #no of words with no polarity
    no_non_polar_words = no_of_words - no_polar_words
    #Number of nouns with a prior polarity
    polar_nouns = len([x for (x,y) in score_dict if  (score_dict[(x,y)][0]>0 or score_dict[(x,y)][1]<0) and y =='n'])
    #Number of nouns with no prior polarity
    non_polar_nouns =  Noofnouns - polar_nouns
    #Number of verbs with a prior polarity
    polar_verbs = len([x for (x,y) in score_dict if  (score_dict[(x,y)][0]>0 or score_dict[(x,y)][1]<0) and y =='v'])
    #Number of verbs with no prior polarity
    non_polar_verbs =  Noofverbs - polar_verbs
     #Number of adverbs with a prior polarity
    polar_adverbs = len([x for (x,y) in score_dict if  (score_dict[(x,y)][0]>0 or score_dict[(x,y)][1]<0) and y =='r'])
    #Number of adverbs with no prior polarity
    non_polar_adverbs =  Noofadv - polar_adverbs
    #Number of adjectives with a prior polarity
    polar_adj = len([x for (x,y) in score_dict if  (score_dict[(x,y)][0]>0 or score_dict[(x,y)][1]<0) and y =='a'])
    #Number of adjectives with no prior polarity
    non_polar_adj =  Noofadj - polar_adj
    
    #positive polarity of nouns
    pos_polar_nouns = [score_dict[(x,y)][0] for (x,y) in score_dict if y=='n']
    pos_polarity_nouns = sum(pos_polar_nouns)
    #negative polarity of nouns
    neg_polar_nouns = [score_dict[(x,y)][1] for (x,y) in score_dict if y=='n']
    neg_polarity_nouns = sum(neg_polar_nouns)
    #Final polarity - choose larger
    polarity_nouns = pos_polarity_nouns if abs(pos_polarity_nouns) > abs(neg_polarity_nouns) else neg_polarity_nouns
     
    #positive polarity of verbs
    pos_polar_verbs = [score_dict[(x,y)][0] for (x,y) in score_dict if y=='v']
    pos_polarity_verbs = sum(pos_polar_verbs)
    #negative polarity of nouns
    neg_polar_verbs = [score_dict[(x,y)][1] for (x,y) in score_dict if y=='v']
    neg_polarity_verbs = sum(neg_polar_verbs)
    #Final polarity - choose larger
    polarity_verbs = pos_polarity_verbs if abs(pos_polarity_verbs) > abs(neg_polarity_verbs) else neg_polarity_verbs
    
     #positive polarity of adjectives
    pos_polar_adj = [score_dict[(x,y)][0] for (x,y) in score_dict if y=='a']
    pos_polarity_adj = sum(pos_polar_adj)
    #negative polarity of nouns
    neg_polar_adj = [score_dict[(x,y)][1] for (x,y) in score_dict if y=='a']
    neg_polarity_adj = sum(neg_polar_adj)
    #Final polarity - choose larger
    polarity_adj = pos_polarity_adj if abs(pos_polarity_adj) > abs(neg_polarity_adj) else neg_polarity_adj
    
    #positive polarity of adverbs
    pos_polar_adv = [score_dict[(x,y)][0] for (x,y) in score_dict if y=='r']
    pos_polarity_adv = sum(pos_polar_adv)
    #negative polarity of adverbs
    neg_polar_adv = [score_dict[(x,y)][1] for (x,y) in score_dict if y=='r']
    neg_polarity_adv = sum(neg_polar_adv)
    #Final polarity - choose larger
    polarity_adv = pos_polarity_adv if abs(pos_polarity_adv) > abs(neg_polarity_adv) else neg_polarity_adv    
    
    #positive polarity of all words
    pos_polar_words = [score_dict[(x,y)][0] for (x,y) in score_dict]
    pos_polarity_total = sum(pos_polar_words)
    #negative polarity of nouns
    neg_polar_words = [score_dict[(x,y)][1] for (x,y) in score_dict]
    neg_polarity_total = sum(neg_polar_words)
    #Final polarity - choose larger
    polarity_total = pos_polarity_total if abs(pos_polarity_total) > abs(neg_polarity_total) else neg_polarity_total
    
    return(np.array([exclm_flag,noofhashtags,no_of_words,capwords,cap_letter_perc,NoofURL,allcapwords,
                     tweet_len,negation_words,
                     no_polar_words,no_non_polar_words,polar_nouns,non_polar_nouns,
                     polar_verbs,non_polar_verbs,polar_adverbs,non_polar_adverbs,
                     polar_adj,non_polar_adj,polarity_nouns,polarity_verbs,
                     polarity_adj,polarity_adv,polarity_total]))
    