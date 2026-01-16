"""Get data from google sheets

# Docs
Quickstart:
https://developers.google.com/sheets/api/quickstart/python
Project creation and mgmt:
https://developers.google.com/workspace/guides/create-project
Create creds:
https://developers.google.com/workspace/guides/create-credentials

# Setup
Google cloud project console:
# TODO: When changing project from 'ohbehave' to a new one, update this URL:
https://console.cloud.google.com/apis/credentials/oauthclient/299107039403-jm7n7m3s9u771dnec1kncsllgoiv8p5a.apps.googleusercontent.com?project=ohbehave

# Data sources
- Refer to config.py
"""
import os
from typing import List, Dict

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd


# Vars
SAMPLE_SPREADSHEET_ID = '1OB0CEAkOhVTN71uIhzCo_iNaiD1B6qLqL7uwil5O22Q'
SAMPLE_RANGE_NAME = '{}'  # sheet name is passed by the CLI
# SAMPLE_RANGE_NAME = SAMPLE_RANGE_NAME + '!A1:B'  # if need subset of columns/rows
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


# Functions
def _get_and_use_new_token(token_path, creds_path):
    """Get new API token"""
    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
    # creds = flow.run_local_server(port=0)
    creds = flow.run_local_server(port=54553)
    # Save the credentials for the next run
    with open(token_path, 'w') as token:
        token.write(creds.to_json())


def _get_sheets_live(google_sheet_name, env_dir) -> Dict:
    """Get sheets from online source"""
    creds = None
    token_path = os.path.join(env_dir, 'token.json')
    creds_path = os.path.join(env_dir, 'credentials.json')
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        try:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                _get_and_use_new_token(token_path, creds_path)
        except RefreshError:
            _get_and_use_new_token(token_path, creds_path)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result: Dict = sheet.values().get(
        spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=SAMPLE_RANGE_NAME.format(google_sheet_name)).execute()

    return result


def get_sheets_data(sheet_name, env_dir) -> pd.DataFrame:
    """Get data from a google sheet."""
    result: Dict = _get_sheets_live(sheet_name, env_dir)

    rows: List[List[str]] = result.get('values', [])
    df = pd.DataFrame(rows[1:]).fillna('')
    df.columns = rows[0]

    return df


if __name__ == '__main__':
    get_sheets_data(sheet_name='category_keywords', env_dir=os.path.join('..', 'env'))