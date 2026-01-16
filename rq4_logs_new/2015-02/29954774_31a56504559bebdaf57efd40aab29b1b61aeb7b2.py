#!/bin/python
# -*- coding: utf-8 -*-

class NotAttributeException(Exception):
    def __init__(self,*args):
        Exception.__init__(self,*args)



class Entity:
    def __init__(self):
        pass

    def inflate(self,attrs):
        signal=True
        for attr_key,attr_val in attrs.items():
            for ele in dir(self):
                signal=True
                if ele==attr_key:
                    signal=False
                    temp=attr_val
                    exec('self.'+attr_key+'=temp')
                    break
            if signal==True:
                raise NotAttributeException('Entity has no attribute '+attr_key)