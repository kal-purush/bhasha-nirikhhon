# -*- coding: utf-8 -*-
# Imports
import cPickle as pickle
import write_functions as w
import nltk.corpus
import string

# Import the sentences that need processing from a pickle file
filename = "raw_datasets/input_file_name.pickle"
unprocessed_sentences = pickle.load(open(filename))
print "File succesfully imported. The file contains %d sentences." %len(unprocessed_sentences)

# # Uncomment in case more pickle files need to be imported
# filename2 = 'filename2.pickle'
# unprocessed_sentences2 = pickle.load(open(filename2))
# print "File succesfully imported. The file contains %d sentences." %len(unprocessed_sentences2)
# for i in unprocessed_sentences2:
#     unprocessed_sentences.append(i)

print "%d lines were imported and are ready for deduplication!"%len(unprocessed_sentences)

# Get default English stopwords and extend with punctuation
stopwords = nltk.corpus.stopwords.words('english')
stopwords.extend(string.punctuation)
stopwords.append('')

def draw_from_list_randomly(dataset, samplesize): # Use if a random set needs to be drawn
    import random
    items = []
    rand_list = random.sample(unprocessed_sentences, samplesize)
    for i in rand_list:
        items.append(i.replace('\n','').decode('utf8','replace'))
    return items
        
def is_ci_token_stopword_set_match(tokenset_a, tokenset_b, threshold=0.95):
    """Check if a and b are matches, using jaccard coefficient."""
    ratio = len(set(tokenset_a).intersection(tokenset_b)) / (float(len(set(tokenset_a).union(tokenset_b)))+0.00000000000000001)
    return (ratio >= threshold)

def generate_bow(sentence):
    tokenset = [token.lower().strip(string.punctuation) for token in nltk.word_tokenize(sentence)\
                if token.lower().strip(string.punctuation) not in stopwords]
    return tokenset

tokensets = []
#Generate all BoW's
for i in unprocessed_sentences:
    tokensets.append(generate_bow(i))

print "Finished creating bow representations."
# Compare all sentences s for similarity with the sentences on index > s

keep_items = []
for i in range(len(unprocessed_sentences)):
    unique_item = True
    for item in range(i+1,len(unprocessed_sentences)-1):
        if is_ci_token_stopword_set_match(tokensets[i], tokensets[item]) == True:
            unique_item = False
    if unique_item == True:
        keep_items.append(unprocessed_sentences[i])
    if i%500 == 0: # Used to track progress of the deduplication
        print "Processed sentences:\t %d/%d"%(i,len(unprocessed_sentences))

print "Amount of kept items: %d"%len(keep_items)

# Write away processed data
w.write_to_pickle("output_file_name", keep_items)