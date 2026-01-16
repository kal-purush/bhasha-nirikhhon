#-*-coding:utf-8-*-
import numpy as np
import os
import random
import tensorflow as tf
from tqdm import tqdm
from tensorflow.python.platform import flags
from utils import sample_task
from utils import make_set_tensor

FLAGS = flags.FLAGS

class DataGenerator(object):
    def __init__(self, query_num_per_class_per_model, support_num_per_class_per_model):
        self.query_num_per_class_per_model = query_num_per_class_per_model
        self.support_num_per_class_per_model = support_num_per_class_per_model

        if FLAGS.data_source == 'PACS':
            self.num_class = FLAGS.num_class
            self.img_size = FLAGS.image_size
            self.input_dim = np.prod(self.img_size)*3
            self.output_dim = self.num_class
            raw_data_dir = FLAGS.data_PATH
            if FLAGS.train:
                self.episode = FLAGS.episode_tr
            else:
                self.episode = FLAGS.episode_ts
    def make_data_tensor(self, train=True):
        """
        :param train: train or not.
        :return: all tasks, composed by dicts e.g.{'support_set': support_set, 'query_set':query_set}
                            which value in dicts is list of data tensors, first element such as support_set[0] is
                            image tensor, the next is label one-hot tensors.
        """
        print('Generating tensor datas...')
        all_tasks = []
        for _ in tqdm(range(self.episode)):
            task = sample_task(query_num_per_class_per_model=FLAGS.query_num, class_num=FLAGS.way_num, support_num_per_class_per_model=FLAGS.support_num)
            #support_set[0] is images, support_set[1] is labels
            support_set, query_set = make_set_tensor(task['support']), make_set_tensor(task['query'])
            all_tasks.append({'support_set': support_set, 'query_set':query_set})
        return all_tasks


