import _init_paths
from collections import Counter
import os
import os.path as osp
import pickle

def get_tree_words(tree):
    words = [tree.word]
    for child in tree.children:
        words += get_tree_words(child)
    return words

train_dir = '/local-scratch/cjc/GenerativeNeuralModuleNetwork/data/CLEVR/clevr-dataset-gen/output/SIZE64/trees/train' 
test_dir = '/local-scratch/cjc/GenerativeNeuralModuleNetwork/data/CLEVR/clevr-dataset-gen/output/SIZE64/trees/test' 

c = Counter()

for tree_file in sorted(os.listdir(train_dir)):
    tree_path = osp.join(train_dir, tree_file)
    with open(tree_path, 'rb') as f:
        tree = pickle.load(f)
    tree_words = get_tree_words(tree)
    c.update(tree_words)
print(c)
print(len(c))

#for tree_file in sorted(os.listdir(test_dir)):
#    tree_words = get_tree_words(tree)
#    c.update(tree_words)
#print(c)