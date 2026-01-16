import os
import sys    #for sys.exit() to help with debugging
import regex as re
import nltk
import string

def clean(corp):
    n_corp = re.sub('<!-- The default annotation set -->(.|\n)*',r'', corp) # removes end half of xml tags
    n_corp = re.sub('<[^<]+>',r'', n_corp)  # removes remaining xml tags
    return n_corp

def values(texts, word, wind):
    co_tot_dic = {}
    tokens = nltk.word_tokenize(texts)
    indices = ([i for i, j in enumerate(tokens) if j == word])  # finds indices of word in texts
    for i in indices:
        frame = tokens[(i-wind):(i+wind)]   # finds window of word based on given window value
        for w in frame:
            if w in co_tot_dic.keys():      # finds and counts all possible collocates in every context
                co_tot_dic[w][0] += 1
            else:
                co_tot_dic[w] = [1]
    for k,v in list(co_tot_dic.items()):        # removes entries seen only three times and punctuation.
        if v[0] <= 3 or k in string.punctuation:    # maybe we can make this an option as well
            del co_tot_dic[k]
    for k in list(co_tot_dic.keys()):                               # finds total counts for possible collocates
        co_tot_dic[k].append(len([w for w in tokens if w == str(k)]))  # append to dictionary list for word "k"
    token_tot = len(tokens)
    word_tot = len(indices)
    return (token_tot, word_tot, co_tot_dic)        # returns 3 value tuple with - token total, word total,\
    #  and dictionary formatted as such --> {collocate:[co-occurrence count, total collocate count], ...}


# def chi_sq():        #placeholders for finished functions
                       #NB instructions for implementing each algorithm can be found in the text!
#                      #I'm hoping that most of the info you need should be extracted.
# def loglike():       #If you need more info from text feel free to run "def values():" within function and modify it.
#                      #NB Please only return 100 best results!!!
#                      #NB and return results in dictionary format with collocates as keys and their values!!!
# def mutual_info():


corps = []
word = input("Please insert word:")             # input for word you want to search
wind = int(input("Please insert window size:")) # input for window size you want to search in
for root,dirs,files in os.walk("nanocorpus"):   # cleans all corpus texts in nanocorpus file
    for name in files:
        with open(os.path.join(root,name), 'r', encoding='utf-8') as corpus:
            corps.append(clean(corpus.read()))
texts = "\n".join(corps)                        # combines texts into one large text
vals = values(texts,word,wind)
# chi_sq(vals)                                  #placeholders for finished stat functions
# loglike(vals)
# mutual(vals)