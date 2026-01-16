from bfex.components.key_generation.key_generation_approach import KeyGenerationApproach
from bfex.components.scraper.scrapp import Scrapp
from bfex.models import *
from bfex.components.data_pipeline.tasks import Task
import nltk
from nltk.tokenize import word_tokenize
import re

class LexiconApproach(KeyGenerationApproach):
    def __init__(self):
        self.approach_id = 5
        self.description = """ Filter out any keywords from text that appear in the lexicon """
        

    def generate_keywords(self, text):
        """ Filter out any keywords from text that appear in the lexicon

        :param text is the text field from a document object
        :return: keywords from the lexicon
        """
        keywords = []
        # TODO: get lexicon from ES
        lexicon = ["string theory"]

        text = re.sub("[^\w']+", ' ', text.lower())
        tokens= nltk.word_tokenize(text)
        words = list(set(tokens))
        bigrams = list(nltk.bigrams(tokens))
        trigrams =list(nltk.trigrams(tokens))

        for word in words:
            if (word in lexicon) and (word not in keywords):
                keywords.append(word)

        for bigram in bigrams:
            search_bigram = " ".join(bigram)
            if (search_bigram in lexicon) and (search_bigram not in keywords):
                keywords.append(search_bigram)

        for trigram in trigrams:
            search_trigram = " ".join(trigram)
            if (search_trigram in lexicon) and (search_trigram not in keywords):
                keywords.append(search_trigram)

        return keywords

        
    def get_id(self):
        return self.approach_id

if __name__ == "__main__":
    from elasticsearch_dsl import connections
    connections.create_connection()
    Keywords.init()
    Document.init()

    search = Document.search().query('match', faculty_id= "356") \
        .query('match', source= "profile") \
        .execute()
    task = LexiconApproach()
    results = task.generate_keywords(search[0].text)
    print(results)