import random
from itertools import combinations
from random import shuffle
import sys
import time
from union_find_solutions import path_compressed_weighted_quick_union_UF

random.seed(42)
def generate_pairs(node_combinations, num):
    shuffle(node_combinations)
    return node_combinations[0:num]

class all_connected_UF(path_compressed_weighted_quick_union_UF):
    '''modified union-find implementation to detect when all elements are connected'''
    def __init__(self, N):
        super(all_connected_UF, self).__init__(N)

    def union(self, nums):
        '''add a connection between nums[0] and nums[1]'''
        super(all_connected_UF, self).union(nums)
        print self.component_size
        #print max(self.component_size[self.give_root(self.component[nums[0]])],
        #self.component_size[self.give_root(self.component[nums[1]])])
def main():
    num_nodes = int(sys.argv[1])
    num_unions = int(sys.argv[2])

    #print list(xrange(num_nodes))
    nodes =  xrange(num_nodes)
    node_combinations = list(combinations(nodes, 2))

    unions = generate_pairs(node_combinations, num_unions)
    
    all_connected = all_connected_UF(num_nodes)
    print all_connected.__doc__

    map(all_connected.union, unions)

if __name__ == "__main__":
    # execute only if run as a script
    main()