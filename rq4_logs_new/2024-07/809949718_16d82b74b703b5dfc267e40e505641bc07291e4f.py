"""
Pull all the links to download NYC Yellow Taxi records
"""
import requests
from bs4 import BeautifulSoup


URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'


def keep_it(link):
    return (
        link.get('href') and 
        'yellow_tripdata' in link.get('href') and 
        link.get('href').endswith('.parquet')
    )


def main():
    # Fetch the page content
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find all links on the page
    links = soup.find_all('a')
    # Filter out links that contain 'yellow_tripdata' and end with '.parquet'
    yellow_taxi_parquet_links = [
        link.get('href') for link in links if keep_it(link)
    ]
    for link in yellow_taxi_parquet_links:
        print(link)


if __name__ == "__main__":
    main()