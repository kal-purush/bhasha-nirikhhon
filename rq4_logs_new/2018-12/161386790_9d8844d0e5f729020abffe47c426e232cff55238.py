from helper_files.py import *
import re
import pickle
from scipy.linalg import svd

def gen_keywordValues(data):
    word_list = []
    for i,r in enumerate(data):
        for word in r:
            if word not in word_list:
                word_list.append(word)
    
    W = np.zeros((len(data), len(word_list)))
    for i,r in enumerate(data):
        for j,word in enumerate(r):
            W[i][j] = W[i][j]+1
    return(W,data,word_list)

def keyword_extraction(data, t = 5, k = 2):
    W,sentences,word_list = gen_keywordValues(data)
    if(k >= len(sentences)):
        k = len(sentences)
    if(t >= len(word_list)):
        t = len(word_list)
    u,s,v = svd(W)
    index = np.argmax(s)
    u = u[:,index]
    v = v[index,:]
    if all(i <= 0 for i in u): u = u*-1
    if all(i <= 0 for i in v): v = v*-1
    u_ind = np.argsort(u)
    v_ind = np.argsort(v)
    return(([(word_list[w], v[w]) for w in v_ind[-t:]], [(sentences[w], u[w]) for w in u_ind[-k:]]))

def main():
    directory = "test_files"
    results = read_dir(directory)
    for r in results:
        business_id = r[1]
        data = r[0]
        keywords = keyword_extraction(data[0])
        filename = business_id + "keywords.pkl"
        with open("keywords/"+filename) as f:
            pickle.dump(data)

main()
    