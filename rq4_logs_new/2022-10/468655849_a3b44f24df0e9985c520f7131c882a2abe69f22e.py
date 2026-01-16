#!/usr/bin/env python
# Copyright (c) 2022 SMHI, Swedish Meteorological and Hydrological Institute.
# License: MIT License (see LICENSE.txt or http://opensource.org/licenses/mit).
"""
Created on 2022-10-06 15:10

@author: johannes
"""


class ModelDoesNotExists(Exception):
    def __init__(self, model_name, detail="Error message!"):
        self.model_name = model_name
        self.detail = ' '.join(detail) \
            if not isinstance(detail, str) else detail
        super().__init__(self.detail)

    def __str__(self):
        if self.model_name:
            return f'ModelException - {self.model_name}: {self.detail}'
        else:
            return self.detail