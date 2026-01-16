'''
Pandas Homework with IMDB data
'''
import pandas as pd

'''
BASIC LEVEL
'''

# read in 'imdb_1000.csv' and store it in a DataFrame named movies
movies=pd.read_csv('imdb_1000.csv')

# check the number of rows and columns
movies.shape

# check the data type of each column
movies.dtypes

# calculate the average movie duration
movies.duration.mean()

# sort the DataFrame by duration to find the shortest and longest movies
movies.sort('duration').iloc[[0,-1],:]
'''
INTERMEDIATE LEVEL
'''

# count how many movies have each of the content ratings
movies.content_rating.value_counts()

# convert the following content ratings to "UNRATED": NOT RATED, APPROVED, PASSED, GP
movies.content_rating.replace(['NOT RATED','APPROVED','PASSED','GP'],'UNRATED',inplace=True)

# convert the following content ratings to "NC-17": X, TV-MA
movies.content_rating.replace(['X','TV-MA'], 'NC-17',inplace=True)

# count the number of missing values in each column
movies.isnull().sum()

# if there are missing values: examine them, then fill them in with "reasonable" values
movies[movies.content_rating.isnull()] #Check whcih movies have NaN value for rating 
movies.content_rating.fillna(value='R', inplace=True) #replaces NaN values with "R"

# calculate the average star rating for movies 2 hours or longer,
round(movies[movies.duration>=120].star_rating.mean(),1)

# and compare that with the average star rating for movies shorter than 2 hours
round(movies[movies.duration<120].star_rating.mean(),1)

#use a visualization to detect whether there is a relationship between star rating and duration
movies.plot(kind='scatter', x='star_rating' , y='duration', alpha=0.4)

# calculate the average duration for each genre
movies.groupby('genre').duration.mean()
'''
ADVANCED LEVEL
'''
import matplotlib.pyplot as plt

# visualize the relationship between content rating and duration
movies.content_rating.value_counts().plot(kind='bar')

# determine the top rated movie (by star rating) for each genre
movies.sort('star_rating', ascending=False).groupby('genre').first()

# check if there are multiple movies with the same title, and if so, determine if they are actually duplicates
duplicate_movietitles=movies[movies.title.duplicated()].title
movies[movies.title.isin(duplicate_movietitles)]

# calculate the average star rating for each genre, but only include genres with at least 10 movies
#SOLUTION 1
movie10=movies.genre.value_counts()
top_genres=movie10[movie10>=10].index
top_movies=movies[movies.genre.isin(top_genres)]
top_movies.groupby('genre').star_rating.agg(['count','mean'])

#SOLUTION 2
movie_rating_avg=movies.groupby('genre').star_rating.agg(['count','mean'])
movie_rating_avg[movie_rating_avg['count']>=10]

'''
BONUS
'''

# Figure out something "interesting" using the actors data!