

import pandas as pd
from forex_python.converter import CurrencyRates, RatesNotAvailableError
import matplotlib.pyplot as plt
import csv
import json

def get_config(config_file_path):
    """
    Read config file and return config dictionary

    parameters
    ----------
    config_file_path: json confuguration file path
    
    return
    ----------
    config
    """

    with open(config_file_path, "r") as fp:
        config = json.load(fp)

    return config



def get_movies_by_metascore(data: pd.DataFrame, genre: str, n_percentile: int, topN = True):
    """
    Read data file to get movies by percentile of metascore for specific genre.
    By deafault it returns top n movies otherwise it returns bottom n movies.
    
    parameters
    ----------
    data: pd.DataFrame 
    genre: str        
    n_percentile: int
    topN : Boolean (Default is True)
    
    returns
    ----------
    DataFrame
    
    """
    
    condition = data['genres'].str.contains(genre)
    condition = condition.fillna(False)
    genre_data = data[['title','metascore','genres']][condition]
    
    condition = genre_data['metascore'] != -1
    genre_data = genre_data[condition]
    
    metascore_desc = genre_data.describe(percentiles = [n_percentile/100, (100-n_percentile)/100])
    
    if topN:
        top_n = metascore_desc['metascore']['{}%'.format(100-n_percentile)]
        top_n_movies = genre_data[genre_data['metascore'] >= top_n]
        return top_n_movies
    else:
        bottom_n = metascore_desc['metascore']['{}%'.format(n_percentile)]
        bottom_n_movies = genre_data[genre_data['metascore'] <= bottom_n]
        return bottom_n_movies





def get_movies_by_imdb(data: pd.DataFrame, genre: str, n_percentile: int, topN = True):
    """
    Read data file to get movies by percentile of metascore for specific genre.
    By deafault it returns top n movies otherwise it returns bottom n movies.
    
    parameters
    ----------
    data: pd.DataFrame 
    genre: str        
    n_percentile: int
    topN : Boolean (Default is True)
    
    returns
    ----------
    DataFrame
    """
    condition = data['genres'].str.contains(genre)
    condition = condition.fillna(False)
    genre_data = data[['title','imdb user rating','genres']][condition]
    
    condition = genre_data['imdb user rating'] != -1
    genre_data = genre_data[condition]
    imdb_desc = genre_data.describe(percentiles = [n_percentile/100, (100- n_percentile)/100])
    
    if topN:
        top_n = imdb_desc['imdb user rating']['{}%'.format(100- n_percentile)]
        top_n_movies = genre_data[genre_data['imdb user rating'] >= top_n]
        return top_n_movies
    else:
        bottom_n = imdb_desc['imdb user rating']['{}%'.format(n_percentile)]
        bottom_n_movies = genre_data[genre_data['imdb user rating'] <= bottom_n]
        return bottom_n_movies
    
    


def get_oscar_movies(data: pd.DataFrame, year: int):
    """
    Read data file which returns oscar winning movies for specific year
    
    parameters
    ----------
    data: pd.DataFrame 
    year: int
        
    returns
    ----------
    Series
    
    """
    movies_awards = data[['title','awards']]
    
    condition = movies_awards['awards'].fillna('False')
    movies_awards = movies_awards[condition != 'False']
    
    pattern = "Oscar {}"
    condition_oscar_movies = movies_awards['awards'].str.contains(pattern.format(year))
    oscar_movies = movies_awards[condition_oscar_movies]
    
    return oscar_movies['title']




def convert_currency(row):
    """
    Read each row of data frame and return currency value in provided currency rate

    """
    try:
        if row['currency'] == 'USD':
            return row['budget_value']
        else:
            return CurrencyRates().convert(row['currency'], "USD", row['budget_value'])
    except RatesNotAvailableError:
        return None



def get_movies_by_budget(data: pd.DataFrame, limit_n: int, topN=True):
    """
    Read data file to get movies having highest or lowest budget in USD.
    By default it returns highest budget movies. Otherwise if topN if False it
    returns lowest budget movies
    
    parameters
    ----------
    data: pd.DataFrame 
    limit_n: int
    topN: Booloean (Default is True)
    
    returns
    ----------
    series 
    """
    movie_budget = data[['title','budget']]
    
    condition = movie_budget['budget'].fillna('False')
    movie_budget = movie_budget[condition != 'False']
    
    movie_budget['budget'] = movie_budget['budget'].str.replace("$","USD",regex = False)
    movie_budget['budget'] = movie_budget['budget'].str.replace(r"\(estimated\)","",regex = False)

    movie_budget[['currency','budget_value']] = movie_budget['budget'].str.extract(r'^(\D+)(\d[\d,\.]*)')
    movie_budget['budget_value'] = movie_budget['budget_value'].str.replace(',', '').astype(float)
    movie_budget['budget_value'] = movie_budget.apply(convert_currency, axis=1)
    
    if topN:
        movies_budget_sort_ascending = movie_budget.sort_values('budget_value', axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last')
        return movies_budget_sort_ascending['title'].head(limit_n)
    else:
        movies_budget_sort_descending = movie_budget.sort_values('budget_value', axis=0, ascending=False, inplace=False, kind='quicksort', na_position='last')
        return movies_budget_sort_descending['title'].head(limit_n)



def highest_no_movies_by_year(data: pd.DataFrame):
    """
    Read data file to get countries which have highest number of movies release in each year
    
    parameters
    ----------
    data: pd.DataFrame 
    
    returns
    ----------
    DataFrame
    """
    movie_year_country = data[['imdbid','year','countries']].copy()
    movie_year_country.drop(movie_year_country.index[movie_year_country['year'] == -1],inplace = True,axis=0)
    movie_year_country['countries'] = movie_year_country['countries'].str.strip('()').str.split(',')
    movie_year_country = movie_year_country.explode('countries')
    
    country_group_by_mcount = movie_year_country.groupby(['year','countries'])['imdbid'].count().reset_index()
    return country_group_by_mcount.loc[country_group_by_mcount.groupby('year')['imdbid'].idxmax()]




def download_plot_rating_vs_awards(data: pd.DataFrame):
    """
    Read data file to dowanload plot of the imdb user rating vs number of awards received of movies
    
    parameters
    ----------
    data: pd.DataFrame 
    
    """
    imdb_rating_awards = data[['imdb user rating','awards']].copy()
    imdb_rating_awards.drop(imdb_rating_awards.index[imdb_rating_awards['imdb user rating'] == -1],inplace = True,axis=0)
    imdb_rating_awards = imdb_rating_awards.dropna()
    
    imdb_rating_awards['awards'] = imdb_rating_awards['awards'].str.split(',')
    imdb_rating_awards['count'] = imdb_rating_awards.awards.str.len()
    
    plt.scatter(imdb_rating_awards['imdb user rating'],imdb_rating_awards['count'])
    
    plt.xlabel('IMDB User Rating')
    plt.ylabel('Awards Count')
    plt.title('Relationship between IMDB User Rating and Awards Count')
    
    plt.savefig("plot rating vs awards.png")





def get_akas_by_movie_region(data: pd.DataFrame, movie: str, region: str):
    """
    Read data file to get akas of a specified movie in a specified region
    
    parameters
    ----------
    data: pd.DataFrame 
    movie: str
    region: str
    
    returns
    ----------
    Dictionary
    """
    akas_of_movie = data[data['title'] == movie]['akas'].values[0]

    dict_akas_region = {}
    index2 = 0
    #i = 0
    start_index_string = 0

    while(True):
        index1 = akas_of_movie.find("(", start_index_string)
        if index1 == -1:
            break
        akas_region = akas_of_movie[index2:index1].strip()
        index2 = akas_of_movie.find(")", start_index_string)
        region = akas_of_movie[index1+1:index2].split(',')[0].strip()
        if region in dict_akas_region:
            dict_akas_region[region].append(akas_region)
        else:
            dict_akas_region[region] = [akas_region]
        #i = i + 1
        start_index_string = index2 + 1
        index2 = index2 + 2
    
    
    return dict_akas_region

    

def get_movies_by_year(data: pd.DataFrame, year: int, flag: int = 0):
    """
    Read data file which returns movies in specific year if flag is 0,
    movies after year if flag is 1 and movies before year if glag is -1.
    
    parameters
    ----------
    data: pd.DataFrame 
    year: int
    flag: int (Default is 0)
    
    returns
    ----------
    list
    """
    movies_year = data[['title','year']]
    
    movies_year.drop(movies_year.index[movies_year['year'] == -1],inplace = True,axis=0)
    
    movies_year = movies_year.sort_values('year')
    
    if flag == 0:
        return movies_year.loc[movies_year['year'] == year,'title' ].tolist()
    elif flag == 1:
        return movies_year.loc[movies_year['year'] > year,'title' ].tolist()
    elif flag== -1:
        return movies_year.loc[movies_year['year'] < year,'title' ].tolist()
    else:
        raise Exception("Invalid flag number")



def top_director(data: pd.DataFrame):
    """
    Read data file to get director who have directed most number of oscar winning movies
    
    parameters
    ----------
    data: pd.DataFrame 
    
    returns
    ----------
    Series
    """
    movies_awards_directors = data[['title','awards','directors']].copy()
    
    movies_awards_directors.dropna(inplace = True)
    
    pattern =  "Oscar"
    condition_oscar_movies = movies_awards_directors['awards'].str.contains(pattern)
    oscar_movies = movies_awards_directors[condition_oscar_movies]
    
    oscar_movies['directors'] = oscar_movies['directors'].str.split(',')
    oscar_movies = oscar_movies.explode('directors')
    
    
    oscar_movies = oscar_movies.groupby('directors')['title'].count().reset_index()
    return oscar_movies.loc[oscar_movies['title'].idxmax()]



def write_output_to_csv(func, filename):
    """
    Takes a function, and a filename as inputs and writes the output of input function
    to given  csv file.
    """
    def wrapper(*args):
        try:
            
            # Call the input function with the given arguments
            output = func(*args)
            
            if isinstance(output, list):
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(output)
            elif isinstance(output, dict):
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=output.keys())
                    writer.writeheader()
                    writer.writerow(output)
            elif isinstance(output, pd.Series):
                output.to_csv(filename,header = False, index=False)
            elif isinstance(output, pd.DataFrame):
                output.to_csv(filename, index=False)
            else:
                raise ValueError("Output is not in a format that can be written to a CSV file.")
        except ValueError as err:
            print(err)
            
    return wrapper



def write_output_to_json(func, filename, *args):
    # Call the input function with the given arguments
    output = func(*args)
    
    if isinstance(output, list):
        with open(filename, 'w') as jsonfile:
            json.dump(output, jsonfile)
            
    elif isinstance(output, dict):
        json_object = json.dumps(output, indent=4)
        with open(filename, 'w') as jsonfile:
            jsonfile.write(json_object)
    else:
        raise ValueError(""" 
Output is not in a format that can be written to a json file. 
Note: for output of type dataframe or series use 
write_output_to_csv instead.
                         """)

def write_output_to_json(func, filename):
    """
    Takes a function, and a filename as inputs and writes the output of input function
    to given  json file.
    """
    def wrapper(*args):
        try:
            
            # Call the input function with the given arguments
            output = func(*args)
            
            if isinstance(output, list):
                with open(filename, 'w') as jsonfile:
                    json.dump(output, jsonfile)
                    
            elif isinstance(output, dict):
                json_object = json.dumps(output, indent=4)
                with open(filename, 'w') as jsonfile:
                    jsonfile.write(json_object)
            else:
                raise ValueError(""" 
        Output is not in a format that can be written to a json file. 
        Note: for output of type dataframe or series use 
        write_output_to_csv instead.
                                 """)
        except ValueError as err:
            print(err)
            
    return wrapper


def main():
    """
    The output of 1 to 8 tasks is stored into either csv or json file.
    This way task 9 is implemented
    All csv or json files and plot are stored in current directort 
    Returns
    -------
    None.

    """
    
    
    config = get_config("analysis of movies database config file.json")
    
    data_file = config["data_file"]
    data = pd.read_csv(data_file)
    genre = config["genre"]
    percentile = config["percentile"]
    year = config["year"]
    topN = config["topN"]
    movie = config["movie"]
    region = config["region"]
    flag = config["flag"]
    
    
    """ Task 1:  Group movies by genres
    a. Top/bottom n percentile movies according to metascore, where, ‘n’ should be a
        parameter passed to your function. For example, if n is 10, then you will be
        expected to find the movies above 90 percentile (top) and below 10 percentile
        (bottom) for each genre.
    b. Top/bottom n percentile movies according to ‘number of imdb user votes’
    """
    task_1_metascore = get_movies_by_metascore(data,genre,percentile)
    print("Top n movies by metascore for genre {}".format(genre))
    print(task_1_metascore)
    print("==========================================================================================================")
    
    task_1_imdb = get_movies_by_imdb(data,genre,percentile)
    print("Top n movies by imdb user ration for genre {}".format(genre))
    print(task_1_imdb)
    print("==========================================================================================================")
    
    """ Task 2 : Movies who have won an Oscar in a particular year. For example, get the year 
    as a parameter to your function and return all the movies that won an Oscar in that year
    """
    task_2 = get_oscar_movies(data,year)
    print("Oscar winning movies for year {}".format(year))
    print(task_2)
    print("==========================================================================================================")
    
    
    """Task 3: Analyze and return n movies with highest/lowest budget  
    """
    task_3 = get_movies_by_budget(data,topN)
    print("Top highest budget movies")
    print(task_3)
    print("==========================================================================================================")
    

    """Task 4: Which countries have highest number of movies release in each year
    """
    task_4 = highest_no_movies_by_year(data)
    print("Countries having highest numver of movies release in each year")
    print(task_4)
    print("==========================================================================================================")
    
    """Task 5: Analyze if there is any relationship between the imdb user rating and number of awards
    received 
    """
    print("Download plot for the imdb user rating and number of awards")
    download_plot_rating_vs_awards(data)
        
    
    """Task 6: Return akas of a specified movie in a specified region
    """
    task_6 = get_akas_by_movie_region(data, movie,region)[region]
    print("asak for movie {}, region {} is ".format(movie,region))
    print(task_6)
    print("==========================================================================================================")
    
    """ Task 7: Movies released on, before or after a given year (take year as a parameter)
    """
    task_7 = get_movies_by_year(data, year,flag)
    print("Movies released on year {}".format(year))
    print(task_7)
    print("==========================================================================================================")

    """Task 8: Which director has made directed most number of oscar winning movies
    """
    task_8 = top_director(data)
    print("Top directors")
    print(task_8)
    print("==========================================================================================================")
    
    """Task 9 """
    #t1
    # wrap the original function with the wrapper function
    task_1_write_output_to_csv = write_output_to_csv(get_movies_by_metascore,'Task 1 metascore.csv')
    # call the wrapped function
    task_1_write_output_to_csv(data,genre,percentile)
    
    #t2
    task_2_write_output_to_csv = write_output_to_csv(get_oscar_movies,'Task 2.csv')
    task_2_write_output_to_csv(data,year)
    
    #t3
    task_3_write_output_to_csv = write_output_to_csv(get_movies_by_budget,'Task 3.csv')
    task_3_write_output_to_csv(data,topN)
    
    #t4
    task_4_write_output_to_csv = write_output_to_csv(highest_no_movies_by_year,'Task 4.csv')
    task_4_write_output_to_csv(data)

    #t6
    task_6_write_output_to_csv = write_output_to_json(get_akas_by_movie_region,'Task 6.json')
    task_6_write_output_to_csv(data, movie,region)
    
    #t7
    task_7_write_output_to_csv = write_output_to_json(get_movies_by_year,'Task 7.json')
    task_7_write_output_to_csv(data,year,flag)
    
    #t8
    task_8_write_output_to_csv = write_output_to_csv(top_director,'Task 8.csv')
    task_8_write_output_to_csv(data)


if __name__ == '__main__':
    main()

