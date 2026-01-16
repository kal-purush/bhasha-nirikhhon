#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""This is task_01"""


import pet

class Dog(Pet):
    def __init__(self, has_shots, **someargs):
        """"Instance attributes"""
        self.has_shots = False
        self.**someargs = Pet.self