#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 09:05:31 2016

@author: timotheeaupetit
"""
# this must come first
from __future__ import print_function

    
class CityList(list):
    def __init__(self):
        list.__init__(self)
    
    def add_city(self, city):
        self.append(city)