import sys
sys.path.append(sys.path[0] +"/..")
sys.path.append(sys.path[0])
import numpy as np 
import pandas as pd 
import utils.file_wav as file_wav
from utils.data_set_op import Chinese2Pinyin
from utils.dir_op import get_file_name_ls_from_dir,get_child_dir_ls_from_dir,\
    get_file_dict_for_different_type

class DataSetBase():
    def __init__(self, dataset_dir, label_file):
        self.dataset_dir = dataset_dir
        self.label_file = label_file
    def get_child_file_dict(self,root_dir):
        file_dic = get_file_dict_for_different_type(root_dir)


class thchs30(DataSetBase):
    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

class aishell(DataSetBase):
    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

class ST_CMDS(DataSetBase):
    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

class DatasetPrepare:
    def __init__(self,dataset_name):
        if dataset_name=="thchs30":
            self.dataset_name = "thchs30"
            self.dataset_root_dir = sys.path[0] +"/../data_thchs30"
            self.train_dir = self.dataset_root_dir + "/train"
            self.test_dir = self.dataset_root_dir + "/test"
        elif dataset_name=="aishell":
            self.dataset_name = "aishell"
            self.dataset_root_dir = sys.path[0] +"/../data_aishell/"
        elif dataset_name=="primewords":
            self.dataset_name = "thchs30"
            self.dataset_root_dir = sys.path[0] +"/../thchs30/"
        elif dataset_name=="ST-CMDS":
            self.dataset_name = "ST-CMDS"
            self.dataset_root_dir = sys.path[0] +"/../ST-CMDS-20170001_1-OS/"        
        elif dataset_name=="train_set":
            self.dataset_name = "train_set"
            self.dataset_root_dir = sys.path[0] +"/../train_set"
    
    def get_wav_and_label(self):
        for file in get_file_name_ls_from_dir(self.train_dir)