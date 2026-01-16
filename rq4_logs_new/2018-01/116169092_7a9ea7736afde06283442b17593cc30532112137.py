#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 20:21:03 2018

@author: lzabb
"""

import hashlib as hasher

class Block(object):
    
    
    def __init__(self, index, timestamp, data, previous_hash):
        self.index = index
        self.timestamp = timestamp
        self.data = data
        self.previous_hash = previous_hash
        self.hash = self.hash_block()
  
          
    def hash_block(self):
        sha = hasher.sha256(str(self.index) +
                            str(self.timestamp) + 
                            str(self.data) + 
                            str(self.previous_hash))
        return sha.hexdigest()