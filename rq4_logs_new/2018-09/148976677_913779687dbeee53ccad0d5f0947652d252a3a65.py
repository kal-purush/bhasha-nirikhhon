from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime

BSA_SITE = "https://www.stammering.org"
BSA_EVENTS_PAGE = "/get-involved/events"

def get_page_as_html(page_link=BSA_EVENTS_PAGE):
    """
    Performs HTTP get request on page_link or BSA_EVENTS_PAGE if no page link is
    provided and returns html as string
    """
    html = ""
    try:
        url = BSA_SITE + page_link
        r = requests.get(url)
        status = r.status_code
        if status == 200 or status == 500:
            html = r.text
        else:
            print("Got response: ", r.status_code , " trying to access: ", url)
            sys.exit(0)
    except Exception as e:
        print("Error requesting events page")
        print(e)
        sys.exit()
    return html

def get_event_page_as_html(page_link):
    """
    Scrapes the page_link and returns a BeautifulSoup object representation
    """
    html_doc = get_page_as_html(page_link)
    soup = BeautifulSoup(html_doc, 'html.parser')
    return soup

def get_event_description(soup):
    """
    Extracts description from the html soup
    """
    description = soup.find("div", class_="field-items").get_text()
    return description

def get_event_location(soup):
    """
    Extracts location from the html soup
    """
    location = soup.find("div", class_="actual_location").get_text()
    return location

def get_event_coordinates(soup):
    """
    Extracts coordinations from the html soup, in the form of a dict with keys:
    'data-lat', 'data-long', 'data-zoom'.
    >> (This function isn't used yet!)
    """
    coords = soup.find("div", class_="google_map_field_display")
    return {
        'data-lat': coords['data-lat'],
        'data-long': coords['data-long'],
        'data-zoom': coords['data-zoom']
    }

def get_correct_date_format(date, start=True):
    """
    Returns a date object given a string in the format e.g "O8 Sept 2018"

    If the date input is "O8 Sept 2018 - 09 Sept 2018" "start - end", we rely
    on the start boolean to return the correct date.
    e.g Input = "O8 Sept 2018 - 09 Sept 2018"
        >> get_correct_date_format(date, start=True)
        >> Date(2018-09-08)

        >> get_correct_date_format(date, start=False)
        >> Date(2018-09-09)

        Input = "08 Sept 2018"
        >> get_correct_date_format(date, start=False)
        >> Date(2018-09-08)
    """
    date_index = 0 if start else 1
    dates = date.split("-")
    if len(dates) == 1:
        date_index = 0

    date = dates[date_index].strip()
    datetime_object = datetime.strptime(date, '%d %B %Y')

    return str(datetime_object.date())

def get_event_data(raw_event_html):
    """
    Given raw event HTML, return a cleaned up dict with useful info
    """
    event_data = {}

    event_data['summary'] = raw_event_html.h5.string
    date = raw_event_html.find("div","the_dates").string
    event_data['start'] = {
        'date': get_correct_date_format(date, start=True),
        'time-zone': "Europe/London"
    }
    event_data['end'] = {
        'date': get_correct_date_format(date, start=False),
        'time-zone': "Europe/London"
    }
    event_data['link'] = raw_event_html.find("div","link_to").a['href']

    # We need to scrape the event page for extra info!
    page_soup = get_event_page_as_html(event_data['link'])
    event_data['description'] = get_event_description(page_soup)
    event_data['location'] = get_event_location(page_soup)

    return event_data

def scrape_for_events():
    """
    Scrapes the BSA events page and parses the HTML representation to return a
    list of events, each event holding a dict of parsed info e.g:
    event = {
        'summary': # Title
        'start': {'date': , 'time-zone'} # Start-date
        'end': {'date': , 'time-zone'} # Start-date
        'location': # Location
        'link': event url
        'description': # Description
    }
    """
    html_doc = get_page_as_html()

    soup = BeautifulSoup(html_doc, 'html.parser')
    event_regex = re.compile("list-item list-item-i sub_node (.)*")
    event_list_raw = soup.find_all(class_=event_regex)

    events = [ get_event_data(event) for event in event_list_raw ]

    return events