#!/usr/bin/env python
import webbrowser
import os
import re

class Movie():
	''' A movie with default properties. '''

	title = "No title"
	url = "about:blank"
	poster_image = "default_poster.gif"
	
	def __init__(self, **entries):
		self.__dict__.update(entries) # An elegant way to set the object variables
	

# A better way is to use json or yaml to store movies data, but to simplify things we just create a list of python dictionaries.
movie_data = [
	{
		'title': "Zombies",
		'trailer_url': "http://sdaff",
		'poster_image': "poster1.gif",
	},
	{
		'title': "Movie Title",
		'trailer_url': "http://sdaff",
	},
]

# Create a list of Movie objects in cycle
movies = []

for idx, md in enumerate(movie_data):
	try:
		movies.append( Movie(**md)) # '**' unpacks dictionary into keyword arguments
		
	except TypeError:
		raise ValueError("Movie data item #%d is not a dictionary: %s given."	% (idx, type(md)) )
	
	
for m in movies:
	print m.title