# -*- coding: utf-8 -*-
"""
@author: Kyle, Buddy, Igii
"""

import string
from dateutil.parser import parse


movies = readJSON('movies.txt')

###############################################################################

# remove unnecessary keys
for movie in movies:
    movie.pop('critics_consensus', None)
    movie.pop('links', None)
    movie.pop('posters', None)
    movie.pop('alternate_ids', None)

# remove incomplete movies
for movie in list(movies):
    if len(movie) != 12:
        movies.remove(movie)
        
# convert cast and directors to lists of strings
for movie in movies:
    cast = []
    directors = []    
    
    for member in movie['abridged_cast']:
        cast.append(member['name'])
    
    for director in movie['abridged_directors']:
        directors.append(director['name'])
    
    movie['abridged_cast'] = cast
    movie['abridged_directors'] = directors
    
# get release month
for movie in movies:
    if 'theater' in movie['release_dates']:
        date = parse(movie['release_dates']['theater'])
    else:
        date = parse(movie['release_dates']['dvd'])
    
    month = date.strftime("%B")
    movie['month'] = month
    movie.pop('release_dates', None)

# get critic and audience rating
for movie in movies:
    movie['audience_rating'] = movie['ratings']['audience_rating']
    
    if movie['ratings']['critics_rating'] != 'Rotten':
        movie['critics_rating'] = 'Fresh'
    else:
        movie['critics_rating'] = 'Rotten'
    
    movie.pop('ratings', None)

# remove all punctuation and make the synopses lower-case
for movie in movies:
    synopsis = movie['synopsis']
    synopsis = synopsis.lower()
    exclude = set(string.punctuation)
    synopsis = ''.join(e for e in synopsis if e not in exclude)
    synopsis = ' '.join(synopsis.split())
    movie['synopsis'] = synopsis
    
###############################################################################

# find # of cast
cast = []

for movie in movies:
    for member in movie['abridged_cast']:
        cast.append(member)
        
print(len(set(cast))) # 3307

# find # of directors
directors = []

for movie in movies:
    for director in movie['abridged_directors']:
        directors.append(director)
        
print(len(set(directors))) # 1122

# find # of studios
studios = []

for movie in movies:
    for studio in movie['studio']:
        studios.append(studio)
        
print(len(set(studios))) # 58

# find # of words
words = []

for movie in movies:
    for word in movie['synopsis'].split():
        words.append(word)
        
print(len(set(words))) # 17882

# find # of genres & what they are
genres = []

for movie in movies:
    for genre in movie['genres']:
        genres.append(genre)
        
print(len(set(genres))) # 21
print(set(genres))

# find ratings & what they are
ratings = []

for movie in movies:
    ratings.append(movie['mpaa_rating'])
        
print(len(set(ratings))) # 6
print(set(ratings))

# find average runtime


# find average per month


# find average synopsis length


# find # of fresh and rotten movies