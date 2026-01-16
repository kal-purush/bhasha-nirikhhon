from nltk.corpus import wordnet as wn
from bfex.models import *
import io
import string
import nltk 
import os

class FilterKeywords():

    """
    Filter out some words from keywords
    """

    def __init__(self):
        self.task_name = "Filter keywords given keyword set"

    
    def is_requirement_satisfied(self, data):
        """Verifies that the data is acceptable and is list.

        :param keywords: Expectd to be a list object contain sets of keywords
        :returns True if the data is of the form above, else False.
        """
        satisfied = True

        return satisfied

    def run(self,data):

        """ Updates a Keyword object information in Elasticsearch after filtering.
        :param data: list of keyword objects
        :return:  returns True.
        """

        for key_object in data:
            key_search = Keywords.search().query('match', faculty_id=key_object.faculty_id) \
                .query('match' , datasource = key_object.datasource) \
                .query('match', approach_id = key_object.approach_id) \
                .execute()
                
            try:
                keywords = key_search[0]
                filter_keywords(key_object.keywords)

            except IndexError:
                    keywords = Keywords()
                    keywords.faculty_id = key_object.faculty_id
                    keywords.datasource = key_object.datasource
                    keywords.approach_id = key_object.approach_id
                
            keywords.keywords = key_object.keywords
            keywords.save()

        return True

def filter_keywords(keywords):

        #remove any punctuation in the keywords, first by replace any punctuation with space, then remove space
        keywords = [''.join(c for c in s if c not in string.punctuation) for s in keywords]
        keywords = [s for s in keywords if s]
        
        # remove any name in the keywords
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'name.txt')

        name_file = io.open(path, 'r')
        name = name_file.read()
        name = [item.lower() for item in name]

        filtered_keywords = []
        for word in keywords:
            if word not in name:
                filtered_keywords.append(word)
        
        #use nltk pos tag to tag each keyword with tag, use only noun tag

        tag = nltk.pos_tag(filtered_keywords)

        tag_keywords = []

        #results with only keywords with noun tag
        for n in range(len(tag)):
            if tag[n][1] in {"NN","NNS"} :
                tag_keywords.append(tag[n])

        nns_list = []

        for n in range(len(tag_keywords)):
            if tag_keywords[n][1] == "NNS":
                nns_tag = nltk.word_tokenize(tag_keywords[n][0])
                t = nltk.pos_tag(nns_tag)
                if t[0][1] != "NN" :
                    nns_list.append(tag_keywords[n][0])

        keywords_list = []
        for n in range(len(tag_keywords)):
            keywords_list.append(tag_keywords[n][0])
        
        keywords = [x for x in keywords_list if x not in nns_list]


        #remove simliar words in the keyword list.
        for n in range(len(keywords)):
            keywords[n] = keywords[n].replace(' ','_')

        simliar_words = []
        for i in range(len(keywords)):
            for n in range(i+1,len(keywords)):
                w1 = wn.synsets(keywords[i])
                w2 = wn.synsets(keywords[n])
                if w1 and w2:
                    score = w1[0].wup_similarity(w2[0])
                    if score != None and score > 0.9:
                        simliar_words.append(keywords[n])

        for word in keywords:
            if word in simliar_words:
                keywords.remove(word)

        for n in range(len(keywords)):
            keywords[n] = keywords[n].replace('_',' ')

        return keywords


if __name__ == "__main__":
    from elasticsearch_dsl import connections
    connections.create_connection()
    Faculty.init()
    Keywords.init()