import sys
import argparse
import sqlite3
import user_management

from data_analysis import OSMCityScraper as OSMScraper


def dispatch(args):
    conn = connect_to_db(args.sqlite_db)

    user_management.initialize(conn)

    if args.bounding_box is not None:
        print(f'Scraping data from OSM for box {args.bounding_box}')
        OSMScraper.scrape(conn, args.bounding_box)

def connect_to_db(url):
    return sqlite3.connect(url)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse OSM arguments.')
    parser.add_argument('--bounding-box', type=float, nargs=4,
            help='four reals specifying a bounding box to download the data. See http://boundingbox.klokantech.com/. ')
    parser.add_argument('sqlite_db', type=str,
            help='Path to SQLite Database to hold the data. Creates the database if not existing and writes into the database if specified.')

    args = parser.parse_args()

    dispatch(args)
