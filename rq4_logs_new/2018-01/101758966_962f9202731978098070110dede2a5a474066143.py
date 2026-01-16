import numpy as np
from keras.models import Sequential, load_model
import pickle
import yaml
import os
import tensorflow as tf
from utilities import calculate_labels_prediction
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences

def text2cnn(text, maxSequenceLength, tokenizer, vectorizationType):
                test_sequences = tokenizer.texts_to_sequences([text])

                test_text = pad_sequences(test_sequences, maxlen=maxSequenceLength)
                return test_text

class CNN_model():



        def __init__(self, pathToConfigFile,klassebetegnelser):
                self.config = {}
                self.load_config(pathToConfigFile)
                self.klassebetegnelser = klassebetegnelser

                global graph
                graph = tf.get_default_graph()

                self.modelDir = os.path.join("CNN_model","cnn-5000-500-20-20171229173429")

                self.model = load_model(os.path.join(self.modelDir, 'model.bin'))

                with open(self.modelDir + '/tokenizer.pickle', 'rb') as handle:
                        self.tokenizer = pickle.load(handle)
                        # loading label indexes
                with open(self.modelDir + '/label_indexes.pickle', 'rb') as handle:
                        self.labels_index = pickle.load(handle)
                self.inverted_labels_indexes = {}
                for i in self.labels_index.keys():
                        self.inverted_labels_indexes[self.labels_index[i]] = i



        def load_config(self, pathToConfigFile):
                with open(pathToConfigFile, "r") as file:
                        self.config = yaml.load(file)
                self.trainingSetPath = self.config["trainingSetPath"]
                self.vocabSize = self.config["vocabSize"]
                self.maxSequenceLength = self.config["maxSequenceLength"]

                self.batchSize = self.config["batchSize"]
                self.vectorizationType = self.config["vectorizationType"]
                self.epochs = self.config["epochs"]
                self.validationSplit = self.config["validationSplit"]
                self.folderToSaveModels = self.config["folderToSaveModels"]
                self.modelDir = None
                self.lossModel = self.config["lossModel"]
                self.w2vPath = self.config["w2vPath"]
                self.embeddingDim = self.config["embeddingDimensions"]
                self.minNumArticlesPerDewey = self.config["minNumArticlesPerDewey"]
                self.kPreds = self.config["kPreds"]





        def predict(self, text,k):
                text = text2cnn(text, self.maxSequenceLength, self.tokenizer,
                                     self.vectorizationType)
                topN_predictions_probabilities = []
                global graph
                with graph.as_default():
                        predictions = self.model.predict(text)


                for prediction_array in predictions:
                        topN_prob_indexes = np.argsort(-prediction_array)[:k]
                for val in topN_prob_indexes:
                        topN_predictions_probabilities.append(predictions[0][val])
                topN_predictions = calculate_labels_prediction(predictions, k, self.inverted_labels_indexes)
                result = []
                for i in range(len(topN_predictions)):
                        result.append([])
                        result[i].append(topN_predictions[i])
                        result[i].append(str(topN_predictions_probabilities[i]))
                        result[i].append(self.klassebetegnelser.get(str(topN_predictions[i])))

                return result

