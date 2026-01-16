
# coding: utf-8

# In[1]:


#----Importing required libraries----
import os
import math
from nltk.corpus import stopwords
from collections import defaultdict
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer


# In[2]:


#----Function returns the document frequency of the tokens----
def df(file_path, tokens, stemmed_docs):
    word_set = set(tokens)
    term2docfreq = defaultdict(int)
    docs= []
    for word in word_set:
        for doc in stemmed_docs:
            if word in doc:
                term2docfreq[word] += 1
    return term2docfreq


# In[3]:


#----Function returns term frequency of the tokens----
def tf(tokens):
    wordset = set(tokens)
    wordfreq = {}
    for word in wordset:
        wordfreq[word] = tokens.count(word)
    return wordfreq


# In[4]:


#----Function processes the tokens to remove stop words from them & stem it---- 
def processed_list(filtered_tokens):
    processed_tokens = []
    for word in filtered_tokens:
        if word not in stopwords.words('english'):
            processed_tokens.append(stemmer.stem(word))
    return processed_tokens


# In[5]:


#----Function that loads the documents to be use----
def load_data(file_path):
    docs = []
    tokens = []
    x = []
    docname2docid = {}
    docid2docname = {}
    for current, filename in enumerate(os.listdir(file_path)):
        docname2docid[filename] = current
        docid2docname[current] = filename
        file = open(os.path.join(file_path, filename), "r")
        doc = file.read()
        file.close()
        doc = doc.lower()
        tokenizer = RegexpTokenizer(r'[a-z]+[0-9]*')
        token = sorted(processed_list(tokenizer.tokenize(doc)))
        tokens += token
        docs.append(token)
    return tokens, docs, docname2docid, docid2docname, current+1
    


# In[6]:


stemmer = PorterStemmer()
DATA_DIR = os.getcwd()+"\data" # For specifying directory of data


# In[7]:


tokens, docs, docname2docid, docid2docname, n = load_data(DATA_DIR)


# In[8]:


doc2tf = defaultdict(int)
for idx, token in enumerate(docs):
    doc2tf[idx] = tf(docs[idx]) # Calling the tf function for tokens in the doc   


# In[9]:


doc_freq = df(DATA_DIR, tokens, docs) # Calling df function for document frequency


# In[10]:


#----Function returns the inverse document frequency----
def calculate_idf(doc_frequency, tokens, N=n):
    token2idf = {} #{token{document:weight}}
    word_set = set(tokens)
    for word in word_set:
        for doc_id in range(N):
            if word not in doc_frequency: 
                current_weight = 0
            else:
                current_weight = math.log((N/doc_frequency[word]), 10)
            token2idf[word] = {}
            token2idf[word] = current_weight
            
    return token2idf


# In[11]:


token2idf = calculate_idf(doc_freq, tokens, N=n)


# In[12]:


#----Function returns the normalized TF-IDF weight of the tokens----
def calculate_tf_idf_weight(term_frequency, doc_frequency, tokens, N=30):
    token2weight = {} # {token{document:weight}}
    word_set = set(tokens)
    summs = [0]*N
    for word in word_set:
        for doc_id in range(N):
            if word not in term_frequency[doc_id]:
                current_weight = 0
            else:
                current_weight = (1+math.log(term_frequency[doc_id][word], 10))*(math.log((N/doc_frequency[word]), 10))
                summs[doc_id] += current_weight**2
            if word not in token2weight:
                token2weight[word] = {}
            token2weight[word][doc_id] = current_weight
            
    
    for i in range(len(summs)):
        summs[i] = math.sqrt(summs[i])
    return token2weight, summs


# In[13]:


token2weight, summs = calculate_tf_idf_weight(doc2tf, doc_freq, tokens, N=n)


# In[14]:


#----Function returns the normalized weight of the query----
def calculate_query_wght(query, test=True):
    query = query.strip()
    query = query.lower()
    tokenizer = RegexpTokenizer(r'[a-z]+[0-9]*')
    tokens = sorted(processed_list(tokenizer.tokenize(query)))
    query2tf = tf(tokens)
    token2qweight = {} 
    summ = 0
    for word in query2tf:
        token2qweight[word] = 1+math.log(query2tf[word], 10)
        summ += (token2qweight[word]**2)
        if test:
            print("Query weight of", word, "=", token2qweight[word])
            
    summ = math.sqrt(summ)
    for word in token2qweight:
        token2qweight[word] = token2qweight[word]/summ
    return token2qweight
        


# In[15]:


#----Function returns the posting list containg top 10 elements in descending order of weights----
def create_posted_list(token2weight):
    posted_list = defaultdict(list)
    for word in token2weight:
        for doc in token2weight[word]:
            if token2weight[word][doc] != 0:
                posted_list[word].append([doc, token2weight[word][doc]])
    for word in posted_list:
        posted_list[word] = sorted(posted_list[word], key=lambda x: x[1], reverse= True)
    return posted_list


# In[16]:


posted_list = create_posted_list(token2weight) #Calling postings list by passing the weights


# In[17]:


#----Function returns d's cosine similarity score if document d appears in the top-10 elements of every query token----  
def calc_cosine_similarity_one(query, posted_list, N = n):
    query_processed = query.strip()
    query_processed = query_processed.lower()
    tokenizer = RegexpTokenizer(r'[a-z]+[0-9]*')
    tokens = sorted(processed_list(tokenizer.tokenize(query_processed)))
    word_set = set(tokens)
    query_top10 = []
    docs =set([])
    similarity_scores = [0]*N
    token2qweight = calculate_query_wght(query, test=False)
    
    for word in word_set:
        if word in posted_list:
            for doc, wght in posted_list[word]:
                wght = wght/summs[doc]
                similarity_scores[doc] += wght*token2qweight[word]
    return similarity_scores


# In[18]:


##----Function returns d's cosine similarity score if document d doesn't appear in the top-10 elements of some query token t----
def calc_cosine_similarity_two(query, posted_list, N = 30):
    query_processed = query.strip()
    query_processed = query_processed.lower()
    tokenizer = RegexpTokenizer(r'[a-z]+[0-9]*')
    tokens = sorted(processed_list(tokenizer.tokenize(query_processed)))
    word_set = set(tokens)
    query_top10 = []
    docs =set([])
    similarity_scores = [0]*N
    token2qweight = calculate_query_wght(query, test=False)

    for word in word_set:
        if word in posted_list:
            visited = set([])
            for doc, wght in posted_list[word]:
                wght = wght/summs[doc]
                visited.add(doc)
                similarity_scores[doc] += wght*token2qweight[word]
            for doc in range(N):
                if doc in visited:
                    continue
                wght = wght/summs[doc]
                similarity_scores[doc] += wght*posted_list[word][-1][1]
        
    
    return similarity_scores


# In[19]:


#----Function returns d's cosine similarity score if document d doesn't appear in the top-10 elements of any query token t----
def calc_cosine_similarity_three(query, posted_list, N = n):
    query_processed = query.strip()
    query_processed = query_processed.lower()
    tokenizer = RegexpTokenizer(r'[a-z]+[0-9]*')
    tokens = sorted(processed_list(tokenizer.tokenize(query_processed)))
    word_set = set(tokens)
    query_top10 = []
    docs =set([])
    similarity_scores = [0]*N
    token2qweight = calculate_query_wght(query, test=False)
    
    for word in word_set:
        if word in posted_list:
            visited = set([])
            for doc, wght in posted_list[word]:
                visited.add(doc)
            for doc in range(N):
                if doc in visited:
                    continue
                wght = wght/summs[doc]
                similarity_scores[doc] += wght*posted_list[word][-1][1]
    
    
    return similarity_scores


# In[20]:


#----Function returns the document with the best similarity socre----
def get_the_best_document(scores, docid2docname):
    maxi_score = max(scores)
    document_num = scores.index(maxi_score)
    if maxi_score == 0:
        return None, None
    return document_num, maxi_score


# In[21]:


#----Function returns the normalized weights for the tokens----
def normalize_wghts(token2weight):
    for i in range(30):
        for word in token2weight:
            token2weight[word][i] = token2weight[word][i]/summs[i] 
    return token2weight


# In[22]:


token2normalized_wghts = normalize_wghts(token2weight)


# In[23]:


#----Function returns the weight for document and token passed----
def getweight(doc_name, token):
    if token not in token2normalized_wghts:
        return 0
    return token2normalized_wghts[token][docname2docid[doc_name]]


# In[24]:


#---Test Case for getweight---
print("%.12f" % getweight("2012-10-03.txt","health")) # 0.008528366190  


# In[25]:


#---Test Case for getweight---
print("%.12f" % getweight("1960-10-21.txt","reason")) # 0.000000000000


# In[26]:


#---Test Case for getweight---
print("%.12f" % getweight("1976-10-22.txt","agenda")) # 0.012683891289


# In[27]:


#---Test Case for getweight---
print("%.12f" % getweight("2012-10-16.txt","hispan")) # 0.023489163449


# In[28]:


#---Test Case for getweight---
print("%.12f" % getweight("2012-10-16.txt","hispanic")) # 0.000000000000


# In[29]:


#----Function returns the inverse document frequency for the token passed----
def getidf(token):
    if token not in token2idf:
        return -1 
    return token2idf[token]

#---Test Case for getidf---   
print("%.12f" % getidf("health")) # 0.079181246048


# In[30]:


#---Test Case for getidf---
print("%.12f" % getidf("agenda")) # 0.363177902413


# In[31]:


#---Test Case for getidf---
print("%.12f" % getidf("vector")) # -1.000000000000


# In[32]:


#---Test Case for getidf---
print("%.12f" % getidf("reason")) # 0.000000000000


# In[33]:


#---Test Case for getidf---
print("%.12f" % getidf("hispan")) # 0.632023214705


# In[34]:


#---Test Case for getidf---
print("%.12f" % getidf("hispanic")) # -1.000000000000


# In[35]:


#----Function returns the document and the weight after computing the similarity according to the 3 cases----
def query(qstring):
    score_one = calc_cosine_similarity_one(qstring, posted_list, N = n)
    score_two = calc_cosine_similarity_two(qstring, posted_list, N = n)
    score_three = calc_cosine_similarity_three(qstring, posted_list, N = n)

    
    best_doc_one, best_score_one = get_the_best_document(score_one, docid2docname)
    best_doc_two , best_score_two  = get_the_best_document(score_two, docid2docname)
    best_doc_three, best_score_three = get_the_best_document(score_three, docid2docname)
    
    if best_doc_one is None or best_doc_two is None or best_doc_three is None:
        return "None", 0
    
    if best_score_one>=best_score_two and best_score_one>=best_score_three:
        return docid2docname[best_doc_one], best_score_one
    elif best_score_two>=best_score_one and best_score_two>=best_score_three:
        return docid2docname[best_doc_two], best_score_two
    elif best_score_three>=best_score_one and best_score_three>=best_score_two:
        return docid2docname[best_doc_three], best_score_three


# In[36]:


#---Test Case for query---
print("(%s, %.12f)" % query("health insurance wall street")) # (2012-10-03.txt, 0.033877975254)


# In[37]:


#---Test Case for query---
print("(%s, %.12f)" % query("particular constitutional amendment"))  # (fetch more, 0.000000000000)


# In[38]:


#---Test Case for query---
print("(%s, %.12f)" % query("terror attack")) # (2004-09-30.txt, 0.026893338131) 


# In[39]:


#---Test Case for query---
print("(%s, %.12f)" % query("vector entropy")) # (None, 0.000000000000)
