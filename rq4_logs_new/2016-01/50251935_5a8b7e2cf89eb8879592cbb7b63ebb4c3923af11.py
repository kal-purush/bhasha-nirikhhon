from __future__ import print_function
import httplib2
import os

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
import ipdb
import base64
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import PyRSS2Gen
import unirest
from django.utils.encoding import smart_str, smart_unicode
import os

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Vacant Feeds'
f = open('article_api_key.txt','r')
ARTICLE_API_KEY = f.read()
ARTICLE_API_KEY = ARTICLE_API_KEY.rstrip()


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'vacant-feeds.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def relevant(link):
    irrelevant = ["twitter", "facebook", "login", "unsubscribe"]
    check = []
    for word in irrelevant:
      if word in link:
        check.append(False)
      else:
        check.append(True)
    
    return all(c == True for c in check)

def main():
    """Shows basic usage of the Gmail API.

    Creates a Gmail API service object and outputs a list of label names
    of the user's Gmail account.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)

    messages_list = service.users().messages().list(userId='me').execute()
    # messID = str(messages_list['messages'][1]['id'])
    messID = '1526ed3c3408d44c'
    userID = "me"
    test_email = service.users().messages().get(userId=userID,id=messID).execute()
    email_content = ""
    parts = test_email['payload']['parts']
    for part in parts:
      test_data = part['body']['data']
      decoded=base64.urlsafe_b64decode(test_data.encode('ASCII'))
      email_content += decoded

    soup = BeautifulSoup(email_content,'html.parser')
    unique_content = []
    unique_links = []

    for link in soup.find_all('a'):
      url = link.get('href')
      response = requests.get(url)
      if response.__dict__['_content'] not in unique_content and relevant(str(response.url)):
        unique_content.append(response.__dict__['_content'])
        unique_links.append(str(response.url))
    
    entries = []

    for link in unique_links:
      article_url = link.replace(":", "%3A", 10)
      article_url = article_url.replace("/", "%2F", 10)
      unirest.timeout(20)
      response = unirest.get("https://joanfihu-article-analysis-v1.p.mashape.com/link?entity_description=False&link="+article_url,
            headers={
                      "X-Mashape-Key":ARTICLE_API_KEY,
                      "Accept": "application/json"
                    }
            )

      title = smart_str(response.__dict__['_body']['title'])
      guid = PyRSS2Gen.Guid(link)
      description = smart_str(' '.join(response.__dict__['_body']['summary']))
      rss_item = PyRSS2Gen.RSSItem(title=title, link=link, description=description, guid=guid, pubDate=datetime.now())
      entries.append(rss_item)

    os.remove('feed')
    rss = PyRSS2Gen.RSS2(
        title = "Credo Action's Feed",
        link = "http://credoaction.com/news/home.html",
        description = "This is a test feed base on the Credo Action email newsletter",
        # guid = PyRSS2Gen.Guid("http://credoaction.com/new/home.html"),
        pubDate = datetime.now(),

        items = entries
        )
    rss.write_xml(open("feed", "w"))

if __name__ == '__main__':
    main()