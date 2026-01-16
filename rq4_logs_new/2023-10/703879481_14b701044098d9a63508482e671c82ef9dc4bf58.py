import random, math

"""
Link Object
parameters: nothing
variables: 
    modifier (float): value to multiply the left node's value by when adding to the right node's value
    left_node (Node): the left-most node of the two
    right_node (Node): the right-most node of the two
purpose: 
    serves as the connections between two nodes
"""
class Link:
    
    def __init__(self):
        self.modifier = random.random() * 2 - 1
        self.left_node = None
        self.right_node = None