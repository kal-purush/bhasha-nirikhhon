import pandas as pd 
import difflib
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def vector_similarity_matrix(data):
    selected_features = ['genres', 'keywords', 'tagline', 'cast', 'director']

    for features in selected_features:
        data[features]=data[features].fillna('')
    
    combined_features = data['genres']+' '+data['keywords']+' '+data['tagline']+' '+data['cast']+' '+data['director']

    vectorizer = TfidfVectorizer()

    feature_vector= vectorizer.fit_transform(combined_features)

    similarity = cosine_similarity(feature_vector)

    return feature_vector , similarity


def recommend_movies(movie_name):
    data = pd.read_csv("movies.csv")
    list_of_data = data['title'].tolist()

    find_close_matches = difflib.get_close_matches(movie_name,list_of_data)

    close_match = find_close_matches[0]

    movie_index = data[data.title == close_match]['index'].values[0]

    
    '''with open('similarity_matrix.pkl','rb') as file :
        similarity_score = pickle.load(file)'''

    vectorized_feature , similarity_score = vector_similarity_matrix(data)
    similarity_movies = list(enumerate(similarity_score[movie_index]))

    similarity_movies_sorted = sorted(similarity_movies,key = lambda x:x[1], reverse=True)

    i =1
    for movies in similarity_movies_sorted:
        index = movies[0]
        movie_title = data[data.index == index]['title'].values[0]
        if(i<20):
            
            print(i,".",movie_title, " score-:" , movies)
            i=i+1 


movie_name = input("Enter you favourite movie: ")

recommend_movies(movie_name)