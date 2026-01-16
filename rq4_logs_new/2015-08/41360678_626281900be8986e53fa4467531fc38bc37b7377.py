###############################################################################
# author: Szilard Cserey
# email:  szilard.cserey@gmail.com
# This application implements a Binary Search Tree and it's functions
#
# python bst.py
###############################################################################

import yaml
import io
import os
import sys
import random

class Node:

    def __init__(self, data=None):
        self.left = None
        self.right = None
        self.parent = None
        self.data = data

    def printout(self):
        print 'DATA: %s PARENT: %s' % (self.data, (self.parent.data if self.parent else None))
        print 'DATA: %s LEFT: %s' % (self.data, (self.left.data if self.left else None))
        print 'DATA: %s RIGHT: %s' % (self.data, (self.right.data if self.right else None))
        if self.left:
            self.left.printout()
        if self.right:
            self.right.printout()

    def insert(self, data):
        if self.data:
            if data < self.data:
                if self.left:
                    self.left.insert(data)
                else:
                    self.left = Node(data)
                    self.left.parent = self
            elif data > self.data:
                if self.right:
                    self.right.insert(data)
                else:
                    self.right = Node(data)
                    self.right.parent = self
        else:
            self.data = data

    def get_max(self, max=None):
        if not max:
            max = self.data
        elif self.data > max:
            max = self.data
        if self.left:
            max = self.left.get_max(max)
        if self.right:
            max = self.right.get_max(max)
        return max

    def get_min(self, min=None):
        if not min:
            min = self.data
        elif self.data < min:
            min = self.data
        if self.left:
            min = self.left.get_min(min)
        if self.right:
            min = self.right.get_min(min)
        return min

    def get_size(self):
        size = 1
        if self.left:
            size += self.left.get_size()
        if self.right:
            size += self.right.get_size()
        return size

    def get_depth(self):
        left_depth = 0
        right_depth = 0
        if self.left:
            left_depth = self.left.get_depth()
        if self.right:
            right_depth = self.right.get_depth()

        depth = left_depth if left_depth > right_depth else right_depth
        return (depth + 1) if self.parent else depth

    def serialize(self):
        bst = {self.data: {}}
        if self.left:
            bst[self.data].update({'L': self.left.serialize()})
        if self.right:
            bst[self.data].update({'R': self.right.serialize()})
        return bst

    def bst_to_file(self, bst_file):
        bst = self.serialize()
        with io.open(bst_file, 'w') as stream:
            yaml.dump(bst, stream, default_flow_style=False)

        with io.open(bst_file) as f:
            print f.read()

    def bst_from_file(self, bst_file):
        if not os.path.isfile(bst_file):
            print 'File %s does not exist!' % bst_file
            sys.exit(1)
        with io.open(bst_file) as stream:
            bst_dict = yaml.load(stream)
        bst = self.deserialize(bst_dict)
        bst.printout()

    def deserialize(self, bst_dict):
        if bst_dict.keys():
            k = bst_dict.keys()[0]
            node = Node()
            node.data = k
            for key, value in bst_dict[k].iteritems():
                if key == 'L':
                    node.left = self.deserialize(value)
                    node.left.parent = node
                elif key == 'R':
                    node.right = self.deserialize(value)
                    node.right.parent = node
            return node

    def get_width(self, level):
        if level == 1:
            return 1
        elif level > 1:
            left_width = 0
            right_width = 0
            if self.left:
                left_width = self.left.get_width(level-1)
            if self.right:
                right_width = self.right.get_width(level-1)
            return left_width + right_width

    def get_max_width(self):
        max_width = 0
        height = self.get_depth() + 1
        for level in range(1, height+1):
            width = self.get_width(level)
            print 'width at level %s: %s' % (level, width)
            if width > max_width:
                max_width = width
        return max_width


def main():
    root = Node()
    max = 10
    value_list = range(1, max+1)
    random.shuffle(value_list)

    for value in value_list:
        root.insert(value)

    root.printout()

    print 'MAX: %s' % root.get_max()

    print 'MIN: %s' % root.get_min()

    print 'SIZE: %s' % root.get_size()

    print 'DEPTH: %s' % root.get_depth()

    print 'DICT: %s' % root.serialize()

    print '============BST TO FILE=========='
    root.bst_to_file('bst.yaml')

    print '============BST FROM FILE=========='
    root.bst_from_file('bst.yaml')

    print 'MAX WIDTH: %s' % root.get_max_width()


if __name__ == '__main__':
    main()