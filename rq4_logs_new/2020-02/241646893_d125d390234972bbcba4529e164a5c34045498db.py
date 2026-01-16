import re
import pandas as pd
import numpy as np
from nltk.tokenize import RegexpTokenizer
from gensim.models import Word2Vec
import multiprocessing
from time import time
from whoosh import analysis
from sklearn.cluster import KMeans
import spacy
from collections import Counter


class UnsupervisedLearning:
    def __init__(self):

        # Initialize a spacy obj.
        self.nlp = spacy.load('en', disable=['parser', 'ner'])

        self.df = pd.read_csv("training.1600000.processed.noemoticon.csv", delimiter=',', names=['target', 'id', 'date',
                                                                                             'flag', 'user', 'text'])
        # Shuffle all rows in the dataframe.
        # Specifying drop=True prevents reset_index() from creating a column containing the old index entries.
        self.df = self.df.sample(frac=1).reset_index(drop=True)

        # Dropping all column except "text", and "target".
        self.df.drop(["id", "date", "flag", "user"], axis=1, inplace=True)

        """
        Solution 1 : Using Spacy for pre-processing and cleaning.
        
        self.nlp = spacy.load('en_core_web_sm', disable=['ner', 'parser'])  # disabling Named Entity Recognition for speed
        
        Creating a mask to split the data into train and test.
        self.msk = np.random.randn(len(self.df)) < 2
        self.df_masked = self.df[~self.msk]
        # Clean the data.
        txt = self.preprocessing()

        # Create a dataframe to save the cleaned tweets.
        self.df_clean = pd.DataFrame({'clean': txt})

        # Adding the labels to df_clean dataframe.
        # First Match the indexes.
        self.df_masked.index = self.df_clean.index
        self.df_clean['target'] = self.df_masked['target']

        # Create another mask.
        self.msk_1 = np.random.rand(len(self.df_clean)) < 0.8

        # Splitting to train and test datasets.
        self.train_all = self.df_clean[self.msk_1]
        self.test = self.df_clean[~self.msk_1]

        # Since this is an unsupervised learning, we will drop the target column for the train data.
        self.train_data = pd.DataFrame({'clean': self.train_all["clean"]})
        self.train_data["clean"] = self.train_data["clean"].str.split(" ")

        # Convert train_data to a list of strings.
        self.train_data = self.train_data.values.tolist()
        self.train_data = [x for x in self.train_data if x]


        # For the test data, we assign the text and target column to test_data, and test_label respectively.
        self.test_data, self.test_label = self.test["clean"], self.test["target"]
        """

        # Solution 2 : Uses custom cleaning and pre-processing.
        
        # Pre-processing.
        self.df_cleaned = self.preprocessing()
        
        # Setting a mask to split the data to train and test.
        self.msk = np.random.randn(len(self.df_cleaned)) < 0.8
        
        # Splitting the data.
        self.train_data = self.df_cleaned[self.msk]
        self.test = self.df_cleaned[~self.msk]

        # Dropping the target column for the training data.
        # Because it's an unsupervised learning, so we don't use the target.
        self.train_data.drop("target", axis=1, inplace=True)
        # Convert the trainging data to a list.
        self.train_data = self.train_data["text"].tolist()

        # Split the texts and labels to different data frames.
        self.test_data = pd.DataFrame(self.test['text'])
        self.test_label = pd.DataFrame(self.test['target'])

        
        # Initializing  a word to vector model.
        self.w2v_model = Word2Vec(min_count=20,
                             window=2,
                             size=300,
                             sample=6e-5,
                             alpha=0.03,
                             min_alpha=0.0007,
                             negative=20,
                             iter=30,
                             workers=multiprocessing.cpu_count() - 1
                             )

    def preprocessing(self):
        # Removing all null records.
        if self.df.isnull().sum()["text"] > 0 | self.df.isnull().sum()["target"]:
            self.df.dropna().reset_index(drop=True)
        """
        # Remove Punctuation, and numbers from texts.
        tokenizer = RegexpTokenizer(r'\w+')
        df_local = pd.DataFrame(columns=['text', 'target'])
        for index, row in self.df.iterrows():
            row['text'] = re.sub(r'\d+', '', row['text'])
            result = tokenizer.tokenize(row["text"])
            result = [token.lower() for token in result]
            for word in result:
                if len(word) < 2:
                    result.remove(word)
            df_local.loc[index] = [result, str(row["target"])]
        """
        # Spacy version to lemmatize.
        t = time()
        df_local = pd.DataFrame(columns=['text', 'target'])
        for index, row in self.df.iterrows():
            doc = self.nlp(row['text'])
            result = [token.lemma_ for token in doc if token.lemma_ != '-PRON-']
            result = [s.lower() for s in result if s.isalpha() and len(s) >= 2]

            # removing redundant values from "result".
            result = list(set(result))

            # Add "result" to the local dataframe.
            df_local.loc[index] = [result, str(row["target"])]

            if index == 60000:
                break
        print('Time for pre-processing: {} mins'.format(round((time() - t) / 60, 2)))

        # brief_cleaning = (re.sub("[^A-Za-z']+", ' ', str(row)).lower() for row in self.df_masked['text'])
        # t = time()
        # 
        # txt = [self.cleaning(doc) for doc in self.nlp.pipe(brief_cleaning, batch_size=5000, n_threads=-1)]
        # 
        # print('Time to clean up everything: {} mins'.format(round((time() - t) / 60, 2)))
        # return txt
        return df_local


    def cleaning(self, doc):

        # Lemmatizes and removes stopwords
        # doc needs to be a spacy Doc object
        txt = [token.lemma_ for token in doc if not token.is_stop]
        # Word2Vec uses context words to learn the vector representation of a target word,
        # if a sentence is only one or two words long,
        # the benefit for the training is very small
        if len(txt) > 2:
            return ' '.join(txt)

    def train(self):
        t = time()
        # Build the vocab.
        self.w2v_model.build_vocab(self.train_data, progress_per=10000)
        print('Time to build vocab: {} mins'.format(round((time() - t) / 60, 2)))

        # Train the model.
        t = time()
        self.w2v_model.train(self.train_data, total_examples=self.w2v_model.corpus_count, epochs=20, report_delay=1)
        print("Time to train : {} mins".format(round((time() - t) / 60, 2)))
        self.w2v_model.save("w2v_model_old")

    def cluster(self):
        self.word_vec = self.w2v_model.wv
        self.model = KMeans(n_clusters=2, max_iter=1000, random_state=True, n_init=50).fit(X=self.word_vec.vectors)
        self.word_vec.similar_by_vector(self.model.cluster_centers_[0], topn=10, restrict_vocab=None)
        order_centroids = self.model.cluster_centers_.argsort()[:, ::-1]
        print ("Clustering hahahaha")


    def test_model(self):
        """
        -Gets the vocab from the word vector model.
        -Add each of the vocab's vector representation as a column in the data frame.
        -Add the cluster each of the vocab belongs to as a column in the data frame.

        * By the end of there three stages we get a data frame with the following columns:
        1- vocab, 2- vector representation, 3- cluster number.

        -Then it iterates over the test data frame.
        -iterate over each word of each sentence.
        -checks if that word belongs to one of the clusters.
        -Till it build a vector with cluster numbers, that each corresponding word in a sentence belongs to.
        -e.g. sentence_1 = ["hate", "to", "leave", "you", "happy"]
        -Cluster 0 "Negative" has "hate", "leave"
        -Cluster 1 "Positive" has "happy"
        - "to", and "you" doesn't belong to either of the clusters
        -The resulting vector would be [0, 0, 1]
        -The prediction would be that sentence_1 belongs to cluster 0.
        -Means that sentence_1 is most probably a negative sentence.
        :return:
        """

        self.words = pd.DataFrame(self.word_vec.vocab.keys())
        self.words.columns = ['words']
        self.words['vectors'] = self.words.words.apply(lambda x: self.word_vec.wv[f'{x}'])
        self.words['cluster'] = self.words.vectors.apply(lambda x: self.model.predict([np.array(x)]))
        # cluster_res = []
        # for index, row in self.test_data.iterrows():
        #     local_text = ' '.join(row['text'])
        #     doc = self.nlp(local_text)
        #     result = [token.lemma_ for token in doc if token.lemma_ != '-PORUN-']
        #     result = [s.lower() for s in result if s.isalpha() and len(s) >= 2]
        #     for word in result:
        #         if word in self.words['words'].tolist():
        #             cluster_res.append(str(self.words['cluster'][index][0]))
        #     # Returns count of each element in the list
        #     if cluster_res:
        #         occ_counter = Counter(cluster_res)
        #         most_com = occ_counter.most_common(1)[0][0]
        #         if most_com == 0:
        #             pred = 4
        #         else:
        #             pred = 1
        #         if pred == self.test_label['target'][index]:
        #             print("Was Classified correctly as : ", self.test_label['target'][index])
        #             print("That was the text : ", row['text'])
        #         else:
        #             print("Classification was wrong")

    def retrain_model(self):
        # load the saved model.
        w2v_model_new = Word2Vec(min_count=20,
                                      window=2,
                                      size=300,
                                      sample=6e-5,
                                      alpha=0.03,
                                      min_alpha=0.0007,
                                      negative=20,
                                      iter=30,
                                      workers=multiprocessing.cpu_count() - 1
                                      )
        # w2v_model_new = Word2Vec.load("w2v_model_old")
        test_ls = self.test_data['text'].tolist()
        w2v_model_new.build_vocab(test_ls, progress_per=10000)
        w2v_model_new.train(test_ls, total_examples=self.w2v_model.corpus_count, epochs=20, report_delay=1)
        print("Okaaay, now we finished updating the model")
        wordVec = w2v_model_new.wv
        words = pd.DataFrame(wordVec.vocab.keys())
        words.columns = ['words']
        words['vectors'] = words.words.apply(lambda x: wordVec.wv[f'{x}'])
        words['cluster'] = words.vectors.apply(lambda x: self.model.predict([np.array(x)]))
        cluster_res = []
        correct, non_correct = 0, 0
        for index, row in self.test_data.iterrows():
            # local_text = ' '.join(row['text'])
            # doc = self.nlp(local_text)
            # result = [token.lemma_ for token in doc if token.lemma_ != '-PORUN-']
            # result = [s.lower() for s in result if s.isalpha() and len(s) >= 2]

            # Saving the indexes of the dataframe "words" in a list.
            uniquq_indexes = pd.Index(list(words['words']))
            for word in row['text']:
                if word in words['words'].tolist():
                    # retrieve the "word" index form "unique_indexes" list.
                    word_indx = uniquq_indexes.get_loc(word)
                    cluster_res.append(str(words['cluster'][word_indx][0]))
            # Returns count of each element in the list
            if cluster_res:
                occ_counter = Counter(cluster_res)
                most_com = occ_counter.most_common(1)[0][0]
                cluster_res = []
                if most_com == '0':
                    pred = '0'
                else:
                    pred = '4'
                if pred == self.test_label['target'][index]:
                    print("Was Classified correctly as : ", pred)
                    print("That was the text : ", row['text'])
                    correct += 1
                else:
                    non_correct += 1
                    print("Classification was wrong")
            print("We got {} of correct guesses, and {} of incorrect guesses".format(str(correct), str(non_correct)))


if __name__ == "__main__":
    Unsupervised_obj = UnsupervisedLearning()
    Unsupervised_obj.train()
    # Unsupervised_obj.w2v_model.vocabulary()
    # Unsupervised_obj.w2v_model.wv.most_similar(positive=["mad"])
    Unsupervised_obj.cluster()
    Unsupervised_obj.retrain_model()
    print("This is it!")