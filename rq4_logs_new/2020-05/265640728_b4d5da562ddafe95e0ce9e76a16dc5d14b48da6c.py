from nltk.tokenize import word_tokenize
from indicnlp.tokenize import sentence_tokenize
from nltk.corpus import stopwords
from tkinter import *
import time

def senti(*args):
    stop_words = set(stopwords.words("stopwords.txt"))
    hindi_sent=input1.get()
    sentences = sentence_tokenize.sentence_split(hindi_sent, lang='hi')
    i=0
    len1= len(sentences)

    """Load Positive Words"""
    with open('postive_words.txt', encoding="utf8") as file:
        pos_data = file.read()

    """Load Negative Words"""
    with open('negative_words.txt', encoding="utf8") as file1:
        neg_data = file1.read()

    """Tokenize Positive Words"""
    tokenized_pos = word_tokenize(pos_data)
    positive_len = len(tokenized_pos)
    
    """Tokenize Negative Words"""
    tokenized_neg = word_tokenize(neg_data)
    negative_len = len(tokenized_neg)


    for i in range(0,len1):
        a = sentences[i]    
        """Words Tokenization"""
        tokenized_word = word_tokenize(a)
        print("Input Text",tokenized_word)
        
        """Remove Stop Words"""
        filtered_sent=[]
        for w in tokenized_word:
            if w not in stop_words:
                filtered_sent.append(w)
        filtered_len = len(filtered_sent)
        print("Filtered Word:", filtered_sent)
        
        """Declaring count variables"""
        pos_count = 0
        neg_count = 0
        
        """Comparing Each Word with the positive text file"""
        for i in range(0, filtered_len):
            for j in range(0, positive_len):
                if filtered_sent[i] == tokenized_pos[j]:
                    pos_count = pos_count + 1
        print("Positive count = ",pos_count)
        
        """Comparing Each Word with the Negative text file"""
        for i in range(0, filtered_len):
            for j in range(0, negative_len):
                if filtered_sent[i] == tokenized_neg[j]:
                    neg_count = neg_count + 1
        print("negative count = ",neg_count)
        
        """Conclusion whether the sentence is positive or negative"""
        if pos_count > 0 and neg_count == 0:
            result="positive Sentence"
        elif pos_count > 0 and neg_count % 2 == 0:
            result="Positive Sentence"
        elif pos_count > 0 and neg_count % 2 != 0:
            result="Negative Sentence"
        elif pos_count == 0 and neg_count %2 != 0:
            result="Negative Sentence"
        elif pos_count == 0 and neg_count == 0:
            result="Neutral Sentence"
        else:
            print("Invalid Input")
        print("\n")
    output.delete('0',END)
    output.insert(10,result)
    
r = Tk() 
r.title('Sentimenal Analysis') 
Label(r, text='Input text:').grid(row=0)
Label(r, text='Output:').grid(row=1)
input1 = Entry(r)
output = Entry(r)
input1.grid(row=0, column=1) 
output.grid(row=1, column=1)
v = IntVar()
v.set(1)
buttonS = Button(r, text='Exit', width=25, command=r.destroy) 
buttonS.grid(row=3,column=1)
buttonP = Button(r, text='Execute', width=25, command=senti) 
buttonP.grid(row=3)
input1.insert(10,"यहाँ लिखें")
input1.bind("<Key>",senti)
r.mainloop()