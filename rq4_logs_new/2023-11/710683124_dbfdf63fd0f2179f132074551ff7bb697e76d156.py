# Script with the Chapter class to support the journey

# Module importation
import requests
from bs4 import BeautifulSoup

# Logging
import logging

log = logging.getLogger(__name__)

# =============================================================================

def update_logbook(
    url: str,
) -> list[dict[str,dict[str,str]]]:
    """
    Checks if a new chapter is online and adds it to the logbook
    INPUTS
        url :: url with the list of chapters online
    OUTPUT
        logbook :: list of Chapters online updated
    """
    log.debug(f'Fetching {url} to retrieve the List of Chapters')
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    # New logbook to store the last online info
    logbook = list()
    log.warning('Search criteria may be modified for other manga or site')
    # Look in all the links
    for a in soup.find_all('a', href=True):
        # Only in the links with text and format
        if a.find('div'):
            # Only in links with info in text
            if ' Chapter ' in a.find('div').text:
                # log.debug(f'Found {a.find("div").text}')
                logbook.append(
                    dict(
                        Chapter=dict(
                            url=a['href'],
                            manga=a.find_all('div')[0].text.split(' Chapter ')[0],
                            number=a.find_all('div')[0].text.split(' Chapter ')[1],
                            title=a.find_all('div')[1].text
                        )
                    )    
                )
    log.info(f'Found {len(logbook)} chapters online')
    return logbook


# =============================================================================