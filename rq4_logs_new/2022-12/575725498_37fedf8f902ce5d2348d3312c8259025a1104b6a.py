from selenium import webdriver
import re
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd
import sqlite3

def get_movie_details():
    #read movie name
    df = pd.read_excel('movies.xlsx')
    movie_names = df["Movie"].to_list()
    #load firefox driver
    driver = webdriver.Firefox() 
    #get rottentomatoes url
    driver.get('https://www.rottentomatoes.com/')

    for movie_name in movie_names:
        searchBox = driver.find_element(By.CLASS_NAME,'search-text')
        searchBox.clear()

        searchBox.send_keys(movie_name)
        time.sleep(5) # Let the user actually see something!
        submitButton = driver.find_element(By.CLASS_NAME,'search-submit')
        submitButton.click()

        # search-result movie only
        search_result = driver.find_elements(By.XPATH,'//search-page-result[@type="movie"]/ul/search-page-media-row')

        years = []
        #to get most recent year from movie list
        for releaseyear in search_result:
            title = releaseyear.find_element(By.XPATH, './/a[@slot="title"]').text
            if title == movie_name:
                year = releaseyear.get_attribute('releaseyear')
                years.append(year)
        if years:
            most_recent_year = max(years)

        movie_match = False

        for result in search_result:
            title = result.find_element(By.XPATH, './/a[@slot="title"]').text
            movie_url  = result.find_element(By.XPATH,'.//a[@slot="title"]')
            #to check same movie name
            if title == movie_name:
                movie_match = True
                if result.get_attribute('releaseyear') == most_recent_year:
                    #most recent release movie
                    url = movie_url.get_attribute('href')
                    driver.get(url)
                    genre = driver.find_element(By.XPATH,'//div[@class="meta-value genre"]').text
                    storyline = driver.find_element(By.XPATH,'//div[@id="movieSynopsis"]').text
                    audiencescore = driver.find_element(By.XPATH,'//score-board[@ class="scoreboard"]').get_attribute('audiencescore')
                    tomatometerscore = driver.find_element(By.XPATH,'//score-board[@ class="scoreboard"]').get_attribute('tomatometerscore')
                    rating = driver.find_element(By.XPATH,'//score-board[@class="scoreboard"]/a[@class="scoreboard__link scoreboard__link--audience"]').text

                    all_reviews = driver.find_elements(By.XPATH,'//review-speech-balloon')
                    movie_name = movie_name+ "("+ most_recent_year +")"

                    review_1=''
                    review_2 = ''
                    review_3 = ''
                    review_4 = ''
                    review_5 = '' 

                    for idx, review in enumerate(all_reviews):
                        review = review.get_attribute('reviewquote')
                        if idx == 0:
                            review_1 = review
                        if idx == 1:
                            review_2 = review
                        if idx == 2:
                            review_2 = review
                        if idx == 3:
                            review_3 = review
                        if idx == 4:
                            review_4 = review
                        if idx == 5:
                            review_5 = review

                        
                    status = 'success'
                    
                    data = [(movie_name,audiencescore,tomatometerscore,genre,rating,storyline,review_1,review_2,review_3,review_4,review_5,status)]
                    #insert the extract data into sqllite3 db
                    insert_data(data)
                    break
        #if movies does not found.
        if movie_match== False:
            status = 'No exact match found'
            movie_name = movie_name
            audiencescore,tomatometerscore,genre,rating,storyline,review_1,review_2,review_3,review_4,review_5 = '','','','','','','','','',''
            data = [(movie_name,audiencescore,tomatometerscore,genre,rating,storyline,review_1,review_2,review_3,review_4,review_5,status)]
            #insert the extract data into sqllite3 db
            insert_data(data)


    driver.close()
    driver.quit()

def insert_data(data):
    # data = [(genre,rating,storyline)]
    c.executemany('INSERT INTO movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', data)
    print("data inserted !!!")

if __name__=="__main__":
    conn = sqlite3.connect('automation.db') # creating database
    c = conn.cursor()
    c.execute('''CREATE TABLE  IF NOT EXISTS movies
                (movie_name text, audiencescore text,tomatometerscore text, genre text, rating text, storyline text,
                review_1 text, review_2 text, review_3 text, review_4 text, review_5 text, status text)''')

    get_movie_details()

    conn.commit()
    conn.close()
    exit()
