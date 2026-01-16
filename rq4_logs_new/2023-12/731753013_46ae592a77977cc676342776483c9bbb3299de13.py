import os
import pickle
from datetime import date
from datetime import datetime, timedelta

import sys

#Gmail API Utils
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

#needed for decoding
from base64 import urlsafe_b64decode


SCOPES = ['https://mail.google.com/']


def parse_arguments():
    if len(sys.argv) != 3:
        raise IndexError('Wrong parameters. Usage is "python3 *filename* *email address* *path to credentials*')

    email = sys.argv[1]
    creds = sys.argv[2]
    return email, creds


def gmail_authenticate(cred_path):
    creds = None

    #Allow the user to login - ability to refresh creds is currently not available
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path,SCOPES)
            creds = flow.run_local_server(port = 0)
        #Save these credentials for future 
        with open ('token.pickle','wb') as token:
            pickle.dump(creds, token)
    return build('gmail','v1', credentials = creds)

def search(service, queery):
    result = service.users().messages().list(userId = 'me', q = queery).execute()
    messages = []
    if 'messages' in result:
        messages.extend(result['messages'])
    while 'nextPageToken' in result:
        page_token = result['nextPageToken']
        result = service.users().messages().list(userId = 'me', q= queery, pageToken = page_token).execute()
        if 'messages' in result:
            messages.extend(result['messages'])
    return messages

def read_message (service, message):
    msg = service.users().messages().get(userId='me', id = message['id'], format = 'full').execute()
    
    payload = msg['payload']
    headers = payload.get('headers')
    parts = payload.get('parts')

    for header in headers:
        name = header.get('name').lower()
        value = header.get('value')

        if name == 'from':
            print ('From:', value)
        if name == 'subject':
            print ('Subject', value)
    parse_parts(service, parts, message)

def parse_parts(service, parts, message):
    if parts:
        for part in parts:
            mimeType = part.get('mimeType')
            body = part.get('body')
            data = body.get('data')

            if part.get('parts'):
                parse_parts(service, part.get('parts'), message)
            if mimeType == 'text/plain':
                if data:
                    text = urlsafe_b64decode(data).decode()
                    read = input('Would you like to read this email? Please enter y or n:')
                    if read.lower() == 'y':
                        print(text)
            else:
                print ('Emails of type' , mimeType, 'are not yet supported')


def main():

    email, cred_path = parse_arguments()
    service = gmail_authenticate(cred_path)

    #Obtain and parse today and tomorrows date
    presentday = datetime.now() 
    tomorrow = presentday + timedelta(1)
    years = ['2018','2019','2020','2021','2022']
    for year in years:

        #Form a queery
        queery = 'after:' + year + '/' + str(presentday.month)+'/' + str(presentday.day) + ' before:' + year + '/' + str(tomorrow.month) + '/' + str(tomorrow.day)
        
        #Search for all messages
        messages = search(service, queery)
    

        for message in messages:
            read_message(service, message)
            delete = input ('Delete this message?')
            if delete.lower() == 'y':
                service.users().messages().delete(userId = 'me', id = message['id']).execute()
                os.system('cls')

    input('Finished. Press any key to exit')
main()