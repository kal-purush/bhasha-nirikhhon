import os
import numpy as np
import random

class Dataset(object):

    def __init__(self, dataset_prefix, batch_size):
        self.path_prefix = dataset_prefix
        self.batch_size = batch_size
        self.examples_per_epoch = 10000

        self.metadata = np.load(self.path_prefix + "_meta.npy")[None][0]
        metadata = self.metadata

        self.max_sentence_length = metadata['max_sentence_length']
        self.max_story_length = metadata['max_story_length']
        self.max_query_length = metadata['max_query_length']
        self.dataset_size = metadata['dataset_size']
        self.vocab_size = metadata['vocab_size']
        self.tokens = metadata['tokens']
        self.datasets = metadata['datasets']
        self.decode = metadata['decode']
        
        
    @property
    def steps_per_epoch(self):
        return self.batch_size * self.examples_per_epoch
    
    def load_dataset(self):
        self.train = np.load(self.path_prefix + "_train.npy")
        self.test = np.load(self.path_prefix + "_test.npy")
        
    def next_batch(self, train_or_test):
        current_set = self.train if train_or_test == "train" else self.test
        sample_ids = random.sample(list(np.arange(len(current_set))), self.batch_size)
        return current_set[sample_ids]