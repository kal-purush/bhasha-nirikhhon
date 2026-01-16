import requests
from bs4 import BeautifulSoup

def get_citations_needed_count(URL):
    """
    This function takes in a url and returns a dictionary.
    """
    event_calendar = []
    response = requests.get(URL)

    content = response.content

    soup = BeautifulSoup(content, 'html.parser')

    result = soup.find_all(class_= 'details')
    for el in result:
        course = {'events': {'event': {}}}
        course['events']['event']['name'] = el.find('h1').get_text()
        course['events']['event']['date'] = el.find('h2').get_text() + ' ' + el.find('h2').find_next_sibling('h2').get_text()
        course['events']['event']['location'] = el.find('h2').find_next_sibling().find_next_sibling().get_text()
        course['events']['event']['description'] = el.find('p').get_text().split('.')[0]

        event_calendar.append(course)
    return event_calendar

print(get_citations_needed_count('https://www.codefellows.org/events-calendar/'))