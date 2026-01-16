# -*- coding: utf-8 -*-
"""
-------------------------------------------------
File Name:    SourceCodeFiles
Description:  
Author:       蔡创
Date:         2021/6/23
-------------------------------------------------
"""
import os
from queue import SimpleQueue


class SourceCodeFiles(object):

    def __init__(self, root):
        self.__root = os.path.abspath(root)
        self.__dir_queue = SimpleQueue()
        self.__file_path_list = list()

    @property
    def root(self):
        return self.__root

    @property
    def file_path_list(self):
        self.__get_file_list_help()
        return self.__file_path_list

    def __get_file_list_help(self):
        current_path = self.__root
        while True:
            temp_name_list = os.listdir(current_path)
            for temp_name in temp_name_list:
                temp_path = os.path.join(current_path, temp_name)
                if os.path.isdir(temp_path):
                    self.__dir_queue.put(temp_path)
                else:
                    self.__file_path_list.append(temp_path)
            if not self.__dir_queue.empty():
                current_path = self.__dir_queue.get_nowait()
            else:
                break