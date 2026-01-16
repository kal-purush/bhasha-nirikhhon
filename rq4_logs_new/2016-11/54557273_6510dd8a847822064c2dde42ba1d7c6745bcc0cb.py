#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Ivo Romario <ivo.romario@gmail.com>
# License:

from node import Node


class BinarySearchTree(object):

    def __init__(self, root=None):
        self.root = root if root else None
        self.size = 0

    def __len__(self):
        return self.size

    def is_empty(self):
        return self.root == None

    def add(self, value):
        new_node = Node(value)

        if self.root is None:
            self.root = new_node
        else:
            # search for a place to insert, starting from root node
            parent = self.root
            while parent.value != value:
                if value < parent.value:
                    if parent.left is None:
                        break
                    parent = parent.left
                elif value > parent.value:
                    if parent.right is None:
                        break
                    parent = parent.right
                else:
                    return

            new_node.parent = parent

            if value < parent.value:
                parent.left = new_node
            else:
                parent.right = new_node
        self.size += 1

    def remove(self, value):
        pass