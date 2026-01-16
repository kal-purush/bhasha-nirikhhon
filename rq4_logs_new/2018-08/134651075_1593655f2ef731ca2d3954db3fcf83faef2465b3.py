#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 10:40:19 2018

@author: yiqian
"""


class Node(object):
  
  def __init__(self, val):
    self.val = val
    self.right = None
    self.left = None
    

def visit(root):
  
  stack = []
  stack.append(root)
  previous = None
  i = 0
  while stack != None:
    print(i)
    current = stack[i]
    if (current.left == None and current.right == None) or (previous != None and (previous==current.left or previous == current.right)):
      print(current.val)
      previous = current
      stack.pop()
      i -= 1
    else:
      if current.right != None:
        stack.append(current.right)
        i += 1
      if current.left != None:
        stack.append(current.left)
        i += 1
        
        
def main():
  root = Node(2)
  
  visit(root)
  
main()