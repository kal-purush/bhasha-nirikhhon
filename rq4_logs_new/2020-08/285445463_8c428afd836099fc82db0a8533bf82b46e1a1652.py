import pytest
from webscraping.thomas.tomz_scraper import data_harvest
from unittest import mock
# from unittest.mock import patch



def test_tomz_scraper_exists():
    assert data_harvest


def test_data_harvest_pass():
    with mock.patch('webscraping.thomas.tomz_scraper.requests.get') as mock_get:
        mock_get.return_value.ok = True
        response = data_harvest(mock_get)
    assert response == []
    

def test_data_harvest_pass_fail():
    with mock.patch('webscraping.thomas.tomz_scraper.requests.get') as mock_get:
        mock_get.return_value.ok = False
        response = data_harvest(mock_get)
    assert response == None


# @pytest.mark.skip
def test_data_harvest_returns_data():

    with mock.patch('webscraping.thomas.tomz_scraper.requests.get') as mock_get:
        mock_get.return_value.json.return_value = { "courses" :[{'course': {'code': 'seattle-code-102n53', 'title': 'Code 102: Intro to Software Development', 'startDate': '2020-10-05', 'endDate': '2020-10-15', 'quarter': '4', 'meetingTimes': 'n', 'track': 'night', 'holidays': '', 'applicationDeadline': '2020-09-21', 'scholarshipDeadline': '2020-09-14', 'cancelled': False, 'family': '102', 'price': 1000.0, 'signupUrl': 'http://learn.codefellows.org/apply-now?utm_source=course-calendar&utm_medium=website&utm_campaign=coding-for-beginners&utm_content=code-102', 'courseStatus': '', 'dailySchedule': '', 'sequence': 'code', 'sequenceAndFamily': 'code-102', 'publicationState': 'Published', 'location': {'name': 'Code Fellows', 'streetAddress': '2901 3rd Ave, Suite 300', 'city': 'Seattle', 'state': 'WA', 'zipCode': '98121'}}}]}
        actual = data_harvest(mock_get)
    assert actual == [{'Code 102: Intro to Software Development': {'code': 'seattle-code-102n53', 'start date': '2020-10-05', 'track': 'night', 'Tuition': '$1,000.00'}}]