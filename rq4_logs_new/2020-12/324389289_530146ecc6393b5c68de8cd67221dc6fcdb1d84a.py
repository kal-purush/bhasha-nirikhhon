"""
This exercise is about a movie collection app.
This is work in progress...
"""
add_new_movie = []
movie_dict_list = {}
movie_info = []
nDigits = 3
m= 'm318'
filepath ='movie_titles_metadata.txt'

def get_movie_dataset():
    movie_dict_list = []
    with open(filepath, encoding="utf8", errors='ignore') as f:
        read_file = f.readlines()[0:]
        for line in read_file:
            movie_dict = {}
            lines = [line.rstrip("\n").split("_") for line in line.split(' +++$+++ ')]
            movie_dict['name'] = lines[1][0]
            movie_dict['year'] = lines[2][0]
            movie_dict['IMDB rating'] = lines[3][0]
            movie_dict['votes'] = lines[4][0]
            movie_dict['genre'] = lines[5]
            movie_dict_list.append(movie_dict)
        return movie_dict, movie_dict_list
     
        
def get_movie_year(dictionary, dict_list, value_to_find):
    movie_info = []
    for element in dict_list:
        for item in element.items():
            if item[1] == value_to_find:   
                movie_info.append(element) 
    return movie_info    


def add_new_data(dictionary_movies, list_dictionary_movies):
    new_name = input('introduce name: ')  
    new_year = input('introduce year: ') 
    new_rating = input('introduce rating: ')  
    new_votes = input('introduce votes: ') 
    new_genre = input('introduce genre: ')
    if None:
        pass
    for element in list_dictionary_movies:
        for key, value in element.items():
            dictionary_movies['name'] = new_name         
            dictionary_movies['year'] = new_year
            dictionary_movies['IMDB rating'] = new_rating
            dictionary_movies['votes'] = new_votes           
            dictionary_movies['genre'] = new_genre
            add_new_movie.append(dictionary_movies)        
        return add_new_movie, new_name, new_year, new_rating, new_votes, new_genre


def get_add_new_movies(element, add_new_data):
    with open(filepath, 'a') as f:
        for element in add_new_data:
            line = m + ' +++$+++ ' +' +++$+++ '.join([value for key, value in element.items()]) + '\n'
        f.write(line)

movie_dict, movie_dict_list = get_movie_dataset()
movie_search = get_movie_year(movie_dict, movie_dict_list, value_to_find = input("Please type a movie's name:  "))
#print(movie_search)
add_new_movie, new_name, new_year, new_rating, new_votes, new_genre = add_new_data(movie_dict, movie_dict_list)
include_new_movies = get_add_new_movies(movie_dict, add_new_movie)

