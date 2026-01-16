import unittest
from unittest import TestCase
from services.equity_parser import EquityParser
from services.redis_services import RedisServices
from services.web_scraper import BseWebScraper


class EquityParserTest(TestCase):
    def test_parser_equity_pass(self):
        parser = EquityParser()
        companies_details = parser.parse("EQ261217.CSV")
        self.assertEqual(len(companies_details), 13)
        self.assertEqual(companies_details[0].get("SC_CODE"), "500002")

    def test_parser_equity_fails(self):
        parser = EquityParser()
        companies_details = parser.parse("")
        self.assertEqual(companies_details.message,
                         ': file "" does not exist, or is not readable')


class RedisServicesTest(TestCase):
    def test_redis_services(self):
        parser = EquityParser()
        companies_details = parser.parse("EQ261217.CSV")
        self.assertEqual(len(companies_details), 13)
        self.assertEqual(companies_details[0].get("SC_CODE"), "500002")
        rdb = RedisServices()
        rdb.post(companies_details)
        comp = rdb.get_top_ten()
        # test get top ten
        self.assertEqual(comp[0], companies_details[0])
        # test get by name
        comp = rdb.get_by_name(companies_details[0].get("SC_NAME"))
        self.assertEqual(comp, companies_details[0])


class WebScraperTest(TestCase):
    def test_csv_downloaded(self):
        bse = BseWebScraper()
        path_to_csv = bse.download_equity_csv()
        self.assertNotEqual(path_to_csv, None)
        parser = EquityParser()
        companies_details = parser.parse(path_to_csv)
        bse.delete_file(path_to_csv)
        self.assertEqual(companies_details[0].keys(), ['SC_CODE',
                                                       'SC_NAME',
                                                       'HIGH',
                                                       'LOW',
                                                       'CLOSE',
                                                       'OPEN'])


if __name__ == "__main__":
    unittest.main()