from services.redis_services import RedisServices
from services.equity_parser import EquityParser
from services.web_scraper import BseWebScraper


def scrape_dowload_store_bhavcopy():
    """- Downloads the Equity bhavcopy zip from the above page
       - Extracts and parses the CSV file in it
       - Writes the records into Redis into appropriate data structures
    (Fields: code, name, open, high, low, close)
    """
    bse = BseWebScraper()
    path_to_csv = bse.download_equity_csv()
    parser = EquityParser()
    companies_details = parser.parse(path_to_csv)
    rdb = RedisServices()
    rdb.post(companies_details)
    bse.delete_file("/tmp/" + path_to_csv)


if __name__ == '__main__':
    scrape_dowload_store_bhavcopy()