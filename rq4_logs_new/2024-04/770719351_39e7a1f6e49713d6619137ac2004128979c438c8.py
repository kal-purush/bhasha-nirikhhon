import requests
import csv
import time
import math
import datetime
import os
import json

print("lol")


baseUrl = 'https://api.jikan.moe/v4/manga'

def fetch_manga_data(page):
    print("good sofar")
    # GET request to the API for a specific page
    url = f'{baseUrl}?page={page}'
    response = requests.get(url)
    if response.status_code == 200:
        print("aokay")
        data = response.json()
        return data['data']
    else:
        print(f'Failed to fetch data from page {page}')
        return []
    
def writeToCsv(filename, fieldnames, rows, mode='w'):
    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        writer.writerows(rows)




def main():
    try:
        total_pages = 2754
        print("1")
        for page in range(1, total_pages + 1):
            print("two")
            time.sleep(4)
            manga_list = fetch_manga_data(page)
            print("three")
            for manga in manga_list:
                manga_data = {
                "bookid": manga['mal_id'],
                "myanimelisturl": manga['url'],
                "title": manga['title'],
                "imgurl": manga['images']['jpg']['large_image_url'],
                "chapters": manga['chapters'],
                "volumes": manga['volumes'],
                "status": manga['status'],
                "publishedstart": manga['published']['from'],
                "publishedend": manga['published']['to'],
                "synopsis": manga['synopsis'],
                "background": manga['background']   
                }
                writeToCsv("Manga.csv", manga_data.keys(), [manga_data],mode='a')

                if manga['authors']:
                    authors = [{"bookid": manga['mal_id'], "Name": author['name'], "Url": author['url']} for author in manga['authors']]
                else:
                    authors = [{"bookid": manga['mal_id'], "Name": "null", "Url": "null"}]
                writeToCsv("Author.csv", authors[0].keys(), authors, mode='a')

                # Extract data for the Genre table
                if manga['genres']:
                    genres = [{"bookid": manga['mal_id'], "type": genre['name']} for genre in manga['genres']]
                else:
                    genres = [{"bookid": manga['mal_id'], "type": "null"}]
                writeToCsv("Genre.csv", genres[0].keys(), genres, mode='a')

                # Extract data for the Theme table
                if manga['themes']:
                    themes = [{"bookid": manga['mal_id'], "type": theme['name']} for theme in manga['themes']]
                else:
                    themes = [{"bookid": manga['mal_id'], "type": "null"}]
                writeToCsv("Theme.csv", themes[0].keys(), themes, mode='a')

                # Extract data for the Demographic table
                if manga["demographics"]:
                    demographics = [{"bookid": manga['mal_id'], "Name": demographic['name']} for demographic in manga['demographics']]
                else:
                    demographics = [{"bookid": manga['mal_id'], "Name": "null"}]
                writeToCsv("Demographic.csv", demographics[0].keys(), demographics, mode='a')

                # Extract data for the Review table
                if manga['score']:
                    scores = [{"bookid": manga['mal_id'], "score": manga["score"]}]
                else:
                    scores = [{"bookid": manga['mal_id'], "score": None}]  
                writeToCsv("Rating.csv", scores[0].keys(), scores, mode='a')

                #time.sleep(4)
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()



    