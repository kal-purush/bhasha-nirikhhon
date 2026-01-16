#!/usr/bin/python

class InputError(Exception):

	def __init__(self, message, errors):
        super(ValidationError, self).__init__(message)
        self.errors = errors